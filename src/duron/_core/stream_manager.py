from __future__ import annotations

import asyncio
from asyncio import CancelledError
from typing import TYPE_CHECKING
from typing_extensions import Any, NamedTuple, final

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from duron._core.ops import StreamObserver
    from duron.codec import Codec
    from duron.tracing._tracer import OpSpan
    from duron.typing import JSONValue, TypeHint


class _StreamInfo(NamedTuple):
    observers: Sequence[StreamObserver]
    dtype: TypeHint[Any]
    name: str | None
    op_span: OpSpan | None


@final
class StreamManager:
    __slots__ = ("_event", "_streams", "_watchers")

    def __init__(self, watchers: Iterable[tuple[str, StreamObserver]]) -> None:
        self._streams: dict[str, _StreamInfo] = {}
        self._watchers: dict[str, list[StreamObserver]] = {}
        self._event = asyncio.Event()

        for name, watcher in watchers:
            self._watchers.setdefault(name, []).append(watcher)

    def create_stream(
        self,
        stream_id: str,
        observer: StreamObserver | None,
        dtype: TypeHint[Any],
        name: str | None,
        op_span: OpSpan | None,
    ) -> None:
        observers = self._watchers.pop(name) if name in self._watchers else []
        if observer:
            observers.append(observer)

        self._streams[stream_id] = _StreamInfo(observers, dtype, name, op_span)
        self._event.set()

    def send_to_stream(
        self, stream_id: str, codec: Codec, offset: int, value: JSONValue
    ) -> bool:
        info = self._streams.get(stream_id)
        if not info:
            return False
        for observer in info.observers:
            observer.on_next(offset, codec.decode_json(value, info.dtype))
        return True

    def close_stream(
        self, stream_id: str, offset: int, exc: Exception | CancelledError | None
    ) -> bool:
        info = self._streams.pop(stream_id, None)
        if not info:
            return False
        self._event.set()

        if isinstance(exc, CancelledError):
            exc = RuntimeError("stream closed", exc)
        for observer in info.observers:
            observer.on_close(offset, exc)
        return True

    def get_info(self, stream_id: str) -> tuple[TypeHint[Any], OpSpan | None] | None:
        if s := self._streams.get(stream_id):
            return (s.dtype, s.op_span)
        return None

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
            self._event.clear()
            await self._event.wait()
