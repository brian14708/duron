from __future__ import annotations

import argparse
import asyncio
import os
import random
import readline
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast
from typing_extensions import override

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
from duron.codec import Codec
from duron.contrib.storage import FileLogStorage

if TYPE_CHECKING:
    from typing import Any

    from duron.codec import JSONValue
    from duron.typing import TypeHint

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


@duron.op
async def do_input() -> str:  # noqa: RUF029
    try:
        return input("> ")  # noqa: ASYNC250
    except EOFError:
        os._exit(0)
    except KeyboardInterrupt:
        os._exit(1)


@duron.fn(codec=PydanticCodec())
async def agent_fn(ctx: duron.Context) -> None:
    console = Console()
    history: list[ChatCompletionMessageParam] = [
        {
            "role": "system",
            "content": "You are a helpful assistant!",
        },
    ]
    while True:
        msg = await ctx.run(do_input)
        history.append({
            "role": "user",
            "content": msg,
        })
        console.print("[bold cyan]     USER[/bold cyan]", msg)
        while True:
            result = await completion(
                ctx,
                messages=history,
            )
            if result.choices[0].message.content:
                console.print(
                    "[bold red]ASSISTANT[/bold red] ", result.choices[0].message.content
                )
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
                    for toolcall in result.choices[0].message.tool_calls or []
                    if toolcall.type == "function"
                ],
            })
            if not result.choices[0].message.tool_calls:
                break

            tasks: list[asyncio.Task[tuple[str, str]]] = []
            for tool_call in result.choices[0].message.tool_calls:
                console.print("[bold yellow]     CALL[/bold yellow]", tool_call.id)
                console.print(tool_call.model_dump_json())
                tasks.append(asyncio.create_task(ctx.run(call_tool, None, tool_call)))
            for id_, tool_result in await asyncio.gather(*tasks):
                console.print("[bold cyan]     TOOL[/bold cyan]", id_)
                console.print(tool_result)
                history.append({
                    "role": "tool",
                    "tool_call_id": id_,
                    "content": tool_result,
                })


@duron.op
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

    log_storage = FileLogStorage(Path("logs") / f"{args.session_id}.jsonl")
    async with agent_fn.invoke(log_storage) as job:
        await job.start()
        await job.wait()


async def completion(
    ctx: duron.Context,
    messages: list[ChatCompletionMessageParam],
) -> ChatCompletion:
    @duron.op(
        metadata={"type": "chat.completions.create"},
    )
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
        None,
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
