from __future__ import annotations

import argparse
import asyncio
import random
import readline
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast
from typing_extensions import Any, override

from openai import AsyncOpenAI, pydantic_function_tool
from openai.lib.streaming.chat import ChatCompletionStreamState
from openai.types.chat import (
    ChatCompletion,
    ChatCompletionMessageParam,
    ChatCompletionMessageToolCallUnion,
)
from pydantic import BaseModel, Field, TypeAdapter
from rich.console import Console

import duron
from duron import Provided, Signal, SignalInterrupt, Stream, StreamWriter
from duron.codec import Codec
from duron.contrib.storage import FileLogStorage
from duron.tracing import Tracer, span

if TYPE_CHECKING:
    from duron.typing import JSONValue, TypeHint

client = AsyncOpenAI()

DEFAULT_MODEL = "gpt-5-nano"


class PydanticCodec(Codec):
    @override
    def encode_json(self, result: object) -> JSONValue:
        return cast(
            "JSONValue",
            TypeAdapter(type(result)).dump_python(
                result,
                mode="json",
                exclude_none=True,
            ),
        )

    @override
    def decode_json(self, encoded: JSONValue, expected_type: TypeHint[Any]) -> object:
        return cast("object", TypeAdapter(expected_type).validate_python(encoded))


@duron.durable(codec=PydanticCodec())
async def agent_fn(
    ctx: duron.Context,
    input_: Stream[str] = Provided,
    signal: Signal[None] = Provided,
    output: StreamWriter[tuple[str, str]] = Provided,
) -> None:
    history: list[ChatCompletionMessageParam] = [
        {
            "role": "system",
            "content": "You are a helpful assistant!",
        },
    ]
    i = 0
    while True:
        msgs: list[str] = [msgs async for msgs in input_.next_nowait(ctx)]
        if not msgs:
            m = await input_.next()
            msgs = [m]

        history.append({
            "role": "user",
            "content": "\n".join(msgs),
        })
        await output.send(("user", "\n".join(msgs)))
        with span(f"Round #{i}"):
            i += 1
            while True:
                try:
                    async with signal:
                        result = await completion(
                            ctx,
                            messages=history,
                        )
                        if result.choices[0].message.content:
                            await output.send((
                                "assistant",
                                result.choices[0].message.content,
                            ))
                        history.append({
                            "role": "assistant",
                            "content": result.choices[0].message.content,
                            "tool_calls": [
                                {
                                    "id": toolcall.id,
                                    "type": "function",
                                    "function": {
                                        "name": toolcall.function.name,
                                        "arguments": toolcall.function.arguments,
                                    },
                                }
                                for toolcall in result.choices[0].message.tool_calls
                                or []
                                if toolcall.type == "function"
                            ],
                        })
                        if not result.choices[0].message.tool_calls:
                            break

                        tasks: list[asyncio.Task[tuple[str, str]]] = []
                        for tool_call in result.choices[0].message.tool_calls:
                            await output.send(("call", tool_call.model_dump_json()))
                            tasks.append(
                                asyncio.create_task(ctx.run(call_tool, tool_call))
                            )
                        for id_, tool_result in await asyncio.gather(*tasks):
                            await output.send(("tool", tool_result))
                            history.append({
                                "role": "tool",
                                "tool_call_id": id_,
                                "content": tool_result,
                            })
                except SignalInterrupt:
                    await output.send(("assistant", "[Interrupted]"))
                    history.append({
                        "role": "assistant",
                        "content": "[Interrupted]",
                    })
                    break


@duron.effect
async def call_tool(params: ChatCompletionMessageToolCallUnion) -> tuple[str, str]:  # noqa: RUF029
    if params.type != "function" or not params.function.name:
        return params.id, '{"status": "error", "message": "Invalid tool call"}'
    tool_name = params.function.name

    if tool_name == "get_temperature":
        return params.id, get_temperature(
            TemperatureInput.model_validate_json(params.function.arguments or "{}")
        ).model_dump_json()
    if tool_name == "get_forecast":
        return params.id, get_forecast(
            ForecastInput.model_validate_json(params.function.arguments or "{}")
        ).model_dump_json()
    return params.id, '{"status": "error", "message": "Unknown tool"}'


async def main() -> None:
    parser = argparse.ArgumentParser(description="Duron Agent Example")
    _ = parser.add_argument(
        "--session-id",
        type=str,
        required=True,
        help="Session ID for log storage",
    )
    args = parser.parse_args()

    log_storage = FileLogStorage(Path("data") / f"{args.session_id}.jsonl")
    async with duron.invoke(
        agent_fn, log_storage, tracer=Tracer(args.session_id)
    ) as job:
        await job.start()
        input_stream: StreamWriter[str] = await job.open_stream("input_", "w")
        signal_stream: StreamWriter[None] = await job.open_stream("signal", "w")
        stream: Stream[tuple[str, str]] = await job.open_stream("output", "r")

        async def reader() -> None:
            console = Console()
            async for role, result in stream:
                match role:
                    case "user":
                        console.print("[bold cyan]     USER[/bold cyan]", result)
                    case "assistant":
                        console.print("[bold red]ASSISTANT[/bold red] ", result)
                    case "tool":
                        console.print("[bold cyan]     TOOL[/bold cyan]", result)
                    case "call":
                        console.print("[bold yellow]     CALL[/bold yellow]", result)
                    case _:
                        console.print("[bold magenta]     ???[/bold magenta]", result)

        async def writer() -> None:
            signal_stream_ = signal_stream
            input_stream_ = input_stream
            while True:
                await asyncio.sleep(0)
                m = await asyncio.to_thread(input, "> ")
                if m.strip():
                    if m == "!":
                        await signal_stream_.send(None)
                    else:
                        await input_stream_.send(m)

        await asyncio.gather(
            job.wait(), asyncio.create_task(reader()), asyncio.create_task(writer())
        )


async def completion(
    ctx: duron.Context,
    messages: list[ChatCompletionMessageParam],
) -> ChatCompletion:
    @duron.effect
    async def _completion(
        messages: list[ChatCompletionMessageParam],
    ) -> ChatCompletion:
        state = ChatCompletionStreamState()
        async for chunk in await client.chat.completions.create(
            messages=messages,
            tools=[
                pydantic_function_tool(
                    TemperatureInput,
                    name="get_temperature",
                    description="Get current temperature for a location",
                ),
                pydantic_function_tool(
                    ForecastInput,
                    name="get_forecast",
                    description="Get weather forecast for a location",
                ),
            ],
            model=DEFAULT_MODEL,
            stream=True,
        ):
            if chunk.object:  # type: ignore[redundant-expr]
                _ = state.handle_chunk(chunk)
        return state.get_final_completion()

    return await ctx.run(
        _completion,
        messages,
    )


# tools


class TemperatureInput(BaseModel):
    location: str = Field(..., description="Location to get weather for")
    unit: Literal["celsius", "fahrenheit"] = Field(
        default="celsius", description="Temperature unit"
    )


class TemperatureOutput(BaseModel):
    location: str
    temperature: float | None
    unit: Literal["celsius", "fahrenheit"]
    status: Literal["success", "error"]
    message: str | None = None


class ForecastInput(BaseModel):
    location: str = Field(..., description="Location for forecast")
    days: int = Field(default=3, ge=1, le=7, description="Number of days (1-7)")


class ForecastDay(BaseModel):
    day: int
    high: float
    low: float
    humidity: int
    wind_speed: int


class ForecastOutput(BaseModel):
    location: str
    forecast: list[ForecastDay]
    status: Literal["success", "error"]


# Simplified tool implementations
def get_temperature(input_data: TemperatureInput) -> TemperatureOutput:
    # Generate random temperature based on realistic ranges
    if input_data.unit == "celsius":
        temp = round(random.uniform(0, 37), 1)
    else:  # fahrenheit
        temp = round(random.uniform(-4, 113), 1)

    return TemperatureOutput(
        location=input_data.location,
        temperature=temp,
        unit=input_data.unit,
        status="success",
    )


def get_forecast(input_data: ForecastInput) -> ForecastOutput:
    forecast: list[ForecastDay] = []
    for i in range(input_data.days):
        high = round(random.uniform(15, 35), 1)
        low = round(random.uniform(5, high - 5), 1)  # Low is always less than high

        forecast.append(
            ForecastDay(
                day=i + 1,
                high=high,
                low=low,
                humidity=random.randint(30, 90),
                wind_speed=random.randint(5, 25),
            )
        )

    return ForecastOutput(
        location=input_data.location, forecast=forecast, status="success"
    )


if __name__ == "__main__":
    _ = readline
    asyncio.run(main())
