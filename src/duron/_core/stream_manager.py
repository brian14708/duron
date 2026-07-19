from __future__ import annotations

import asyncio
from asyncio import CancelledError
from typing import TYPE_CHECKING
from typing_extensions import Any, NamedTuple, final

from duron.errors import InvalidRunStateError

if TYPE_CHECKING:
    from duron._core.ops import StreamObserver, StreamReplayState
    from duron.codec import Codec
    from duron.tracing._tracer import OpSpan
    from duron.typing import JSONValue, TypeHint


class _StreamInfo(NamedTuple):
    observer: StreamObserver | None
    dtype: TypeHint[Any]
    name: str | None
    op_span: OpSpan | None
    error_types: tuple[type[BaseException], ...]
    replay_state: StreamReplayState | None


@final
class StreamManager:
    __slots__ = ("_closed", "_event", "_streams")

    def __init__(self) -> None:
        self._streams: dict[str, _StreamInfo] = {}
        self._event = asyncio.Event()
        self._closed = False

    def create_stream(
        self,
        stream_id: str,
        observer: StreamObserver | None,
        dtype: TypeHint[Any],
        name: str | None,
        op_span: OpSpan | None,
        error_types: tuple[type[BaseException], ...] = (),
        replay_state: StreamReplayState | None = None,
    ) -> None:
        self._streams[stream_id] = _StreamInfo(
            observer, dtype, name, op_span, error_types, replay_state
        )
        self._event.set()

    def send_to_stream(
        self,
        stream_id: str,
        codec: Codec,
        offset: int,
        value: JSONValue,
        *,
        replayed: bool = False,
    ) -> bool:
        info = self._streams.get(stream_id)
        if not info:
            return False
        if replayed and info.replay_state is not None:
            # Event replayed from an interrupted external producer: its
            # re-run will re-offer it, so count it for skipping (see
            # StreamReplayState).
            info.replay_state.remaining += 1
        if info.observer:
            info.observer.on_next(offset, codec.decode_json(value, info.dtype))
        return True

    def close_stream(
        self,
        stream_id: str,
        offset: int,
        exc: Exception | CancelledError | None,
        *,
        replayed: bool = False,
    ) -> bool:
        info = self._streams.pop(stream_id, None)
        if not info:
            return False
        self._event.set()

        if replayed and info.replay_state is not None:
            info.replay_state.remaining += 1
        if isinstance(exc, CancelledError):
            exc = RuntimeError("stream closed", exc)
        if info.observer:
            info.observer.on_close(offset, exc)
        return True

    def get_info(self, stream_id: str) -> tuple[TypeHint[Any], OpSpan | None] | None:
        if s := self._streams.get(stream_id):
            return (s.dtype, s.op_span)
        return None

    def error_types_of(self, stream_id: str) -> tuple[type[BaseException], ...]:
        """Return the declared error types for the stream.

        Returns:
            The error types declared when the stream was created, or an empty
            tuple if the stream is unknown.

        """
        if s := self._streams.get(stream_id):
            return s.error_types
        return ()

    def close(self) -> None:
        """Signal that no further streams will be created.

        Wakes any :meth:`wait_stream` waiter so a host writer blocked on a
        stream the workflow never created fails instead of hanging forever.
        """
        self._closed = True
        self._event.set()

    async def wait_stream(self, name: str) -> str:
        while True:
            match = tuple(
                stream_id
                for stream_id, info in self._streams.items()
                if info.name == name
            )
            if match:
                if len(match) != 1:
                    msg = "multiple streams matched"
                    raise RuntimeError(msg)
                return match[0]
            if self._closed:
                msg = (
                    f"stream {name!r} was never created before the run reached a "
                    "terminal state"
                )
                raise InvalidRunStateError(msg)
            self._event.clear()
            await self._event.wait()
