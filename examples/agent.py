from __future__ import annotations

import argparse
import asyncio
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import TYPE_CHECKING, cast
from typing_extensions import override

from openai import AsyncOpenAI
from openai.lib.streaming.chat import ChatCompletionStreamState
from openai.types.chat import (
    ChatCompletionChunk,
    ChatCompletionMessageParam,
)
from pydantic import TypeAdapter

import duron
from duron import RunOptions, op
from duron.codec import Codec
from duron.contrib.storage import FileLogStorage

if TYPE_CHECKING:
    from typing import Any

    from openai.types.chat import (
        ParsedChatCompletionMessage,
    )

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


@duron.fn(codec=PydanticCodec())
async def agent_fn(ctx: duron.Context) -> None:
    result = await completion(
        ctx,
        messages=[
            {
                "role": "system",
                "content": "You are a helpful assistant!",
            },
            {
                "role": "user",
                "content": "Say hello to Duron.",
            },
        ],
    )
    print(result.content)


async def main() -> None:
    parser = argparse.ArgumentParser(description="Duron Agent Example")
    _ = parser.add_argument(
        "--session-id",
        type=str,
        required=True,
        help="Session ID for log storage",
    )
    args = parser.parse_args()

    log_storage = FileLogStorage(Path("logs") / f"{args.session_id}.json")
    async with agent_fn.invoke(log_storage) as job:
        await job.start()
        await job.wait()


async def completion(
    ctx: duron.Context,
    messages: list[ChatCompletionMessageParam],
) -> ParsedChatCompletionMessage[None]:
    @op(
        checkpoint=True,
        action_type=ChatCompletionChunk,
        return_type=ChatCompletionStreamState | None,
        initial=lambda: None,
        reducer=lambda a, b: (
            s := a or ChatCompletionStreamState(),
            s.handle_chunk(b),
            s,
        )[-1],
    )
    async def _completion_stream(
        prev: ChatCompletionStreamState | None,
        messages: list[ChatCompletionMessageParam],
    ) -> AsyncGenerator[ChatCompletionChunk, ChatCompletionStreamState | None]:
        if prev:
            msg = prev.current_completion_snapshot.choices[0].message
            messages = [
                *messages,
                {
                    "role": "assistant",
                    "content": msg.content,
                    "tool_calls": (
                        {
                            "id": call.id,
                            "type": call.type,
                            "function": {
                                "name": call.function.name,
                                "arguments": call.function.arguments,
                            },
                        }
                        for call in msg.tool_calls
                    )
                    if msg.tool_calls
                    else (),
                },
            ]
        async for chunk in await client.chat.completions.create(
            messages=messages,
            model=DEFAULT_MODEL,
            stream=True,
        ):
            if chunk.object:  # type: ignore[redundant-expr]
                yield chunk

    state = await ctx.run(
        _completion_stream,
        RunOptions(
            metadata={"type": "chat.completions.create"},
        ),
        messages,
    )
    assert state  # noqa: S101
    return state.get_final_completion().choices[0].message


if __name__ == "__main__":
    asyncio.run(main())
