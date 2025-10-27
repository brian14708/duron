from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from typing_extensions import override

from httpx import AsyncClient
from pydantic import BaseModel, TypeAdapter
from pydantic_ai import (
    Agent,
    DeferredToolRequests,
    DeferredToolResults,
    FunctionToolset,
    ModelMessage,
    ModelResponse,
    RunContext,
    TextPart,
)
from rich.console import Console

import duron
from duron.codec import Codec
from duron.contrib.storage import FileLogStorage
from duron.tracing import Tracer, span

if TYPE_CHECKING:
    from duron.typing import JSONValue, TypeHint


class PydanticCodec(Codec):
    @override
    def encode_json(self, result: object, annotated_type: TypeHint[Any]) -> JSONValue:
        return cast(
            "JSONValue",
            TypeAdapter(
                cast("type[object]", annotated_type) if annotated_type else type(result)
            ).dump_python(result, mode="json", exclude_none=True),
        )

    @override
    def decode_json(self, encoded: JSONValue, expected_type: TypeHint[Any]) -> object:
        return cast("object", TypeAdapter(expected_type).validate_python(encoded))


@duron.durable(codec=PydanticCodec())
async def agent_fn(
    ctx: duron.Context,
    input_: duron.Stream[str] = duron.Provided,
    signal: duron.Signal[None] = duron.Provided,
    output: duron.StreamWriter[tuple[str, str]] = duron.Provided,
) -> None:
    """
    Durable agent workflow that runs a PydanticAI agent.

    This function demonstrates:
    - Streaming input/output for interactive chat
    - Signal handling for interruption (send "!" to interrupt)
    - Tool approval workflow (to simulate checkpoint)
    - Durable execution that can be paused and resumed

    Args:
        ctx: Duron context for executing effects
        input_: Stream of user messages
        signal: Signal for interrupting the current round
        output: Stream writer for sending responses
    """
    # Initialize HTTP client for API calls
    deps = Deps(client=AsyncClient())

    # Configure PydanticAI agent with tool approval requirement
    agent = Agent(
        "openai:gpt-5-nano",
        output_type=[str, DeferredToolRequests],
        toolsets=[weather_toolset.approval_required()],
        instructions="Be concise, reply with one sentence.",
        deps_type=Deps,
    )

    # Store conversation history across rounds
    messages: list[ModelMessage] = []

    # Print initial instructions to the output stream
    await output.send((
        "assistant",
        "Agent initialized. Available tools: get_lat_lng, get_weather",
    ))
    await output.send(("assistant", "Send '!' to interrupt the current request."))

    @duron.effect
    async def agent_run(
        user_input: str | None = None,
        deferred_tool_results: DeferredToolResults | None = None,
    ) -> tuple[list[ModelMessage], str | DeferredToolRequests]:
        # wrap for helper and correct typing
        result = await agent.run(
            user_input,
            message_history=messages,
            deps=deps,
            deferred_tool_results=deferred_tool_results,
        )
        return (result.new_messages(), result.output)

    async def run_round(user_input: str) -> None:
        """
        Execute one conversation round, handling tool approval loop.

        Flow:
        1. Run agent with user input
        2. If agent requests tools, approve each one and re-run
        3. Repeat until agent returns a text response
        """
        # Initial agent run with user input
        new_messages, output_val = await ctx.run(agent_run, user_input)
        messages.extend(new_messages)

        # Handle tool approval loop
        while isinstance(output_val, DeferredToolRequests):
            tool_approval = DeferredToolResults()

            # Auto-approve all requested tools (could be manual in production)
            for call in output_val.approvals:
                await output.send(("call", f"{call.tool_name}({call.args})"))
                tool_approval.approvals[call.tool_call_id] = True

            # Re-run agent with tool approvals
            new_messages, output_val = await ctx.run(
                agent_run, deferred_tool_results=tool_approval
            )
            messages.extend(new_messages)

        # Send final text response
        await output.send(("assistant", output_val))

    # Main conversation loop (max 100 rounds)
    for i in range(100):
        # Collect any queued messages without waiting
        it = await input_.next(block=False)
        msgs: list[str] = list(it)

        # If no queued messages, wait for next input
        if not msgs:
            await output.send(("assistant", "[Waiting on user input...]"))
            m = await input_.next(block=True)
            msgs.extend(m)

        # Execute round with tracing and interruption support
        with span(f"Round #{i + 1}"):
            await output.send(("user", "\n".join(msgs)))
            try:
                # Signal context allows interruption via signal stream
                async with signal:
                    await run_round("\n".join(msgs))
            except duron.SignalInterrupt:
                # Handle graceful interruption
                await output.send(("assistant", "[Interrupted]"))
                messages.append(ModelResponse(parts=[TextPart("[Interrupted]")]))


async def main() -> None:
    parser = argparse.ArgumentParser(description="Duron Agent Example")
    _ = parser.add_argument(
        "--session-id", type=str, required=True, help="Session ID for log storage"
    )
    args = parser.parse_args()

    log_storage = FileLogStorage(Path("data") / f"{args.session_id}.jsonl")
    async with duron.Session(log_storage, tracer=Tracer(args.session_id)) as session:
        task = await session.start(agent_fn)
        input_stream: duron.StreamWriter[str] = await task.open_stream("input_", "w")
        signal_stream: duron.StreamWriter[None] = await task.open_stream("signal", "w")
        stream: duron.Stream[tuple[str, str]] = await task.open_stream("output", "r")

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
            task.result(), asyncio.create_task(reader()), asyncio.create_task(writer())
        )


# Toolset


@dataclass
class Deps:
    client: AsyncClient


weather_toolset: FunctionToolset[Deps] = FunctionToolset()


class LatLng(BaseModel):
    lat: float
    lng: float


_ = RunContext[Deps]


@weather_toolset.tool
async def get_lat_lng(ctx: RunContext[Deps], location_description: str) -> LatLng:
    """Get the latitude and longitude of a location.

    Args:
        ctx: The context.
        location_description: A description of a location.

    Returns:
        A LatLng object containing the latitude and longitude.
    """
    r = await ctx.deps.client.get(
        "https://demo-endpoints.pydantic.workers.dev/latlng",
        params={"location": location_description},
    )
    r.raise_for_status()
    return LatLng.model_validate_json(r.content)


@weather_toolset.tool
async def get_weather(ctx: RunContext[Deps], lat: float, lng: float) -> dict[str, Any]:
    """Get the weather at a location.

    Args:
        ctx: The context.
        lat: Latitude of the location.
        lng: Longitude of the location.

    Returns:
        A dictionary containing the temperature and description of the \
                weather.
    """
    # NOTE: the responses here will be random, and are not related to the lat and lng.
    temp_response, descr_response = await asyncio.gather(
        ctx.deps.client.get(
            "https://demo-endpoints.pydantic.workers.dev/number",
            params={"min": 10, "max": 30},
        ),
        ctx.deps.client.get(
            "https://demo-endpoints.pydantic.workers.dev/weather",
            params={"lat": lat, "lng": lng},
        ),
    )
    temp_response.raise_for_status()
    descr_response.raise_for_status()
    return {
        "temperature": f"{temp_response.text} °C",
        "description": descr_response.text,
    }


if __name__ == "__main__":
    asyncio.run(main())
