from __future__ import annotations

import asyncio
from asyncio import CancelledError
from typing import TYPE_CHECKING, NamedTuple
from typing_extensions import Any, final

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

    from duron._core.ops import StreamObserver
    from duron.codec import Codec
    from duron.tracing._tracer import OpSpan
    from duron.typing import JSONValue, TypeHint


class _StreamInfo(NamedTuple):
    observers: Sequence[StreamObserver]
    dtype: TypeHint[Any]
    labels: Mapping[str, str] | None
    op_span: OpSpan | None


@final
class StreamManager:
    __slots__ = ("_event", "_streams", "_watchers")

    def __init__(
        self, watchers: Iterable[tuple[StreamObserver, Sequence[tuple[str, str]]]]
    ) -> None:
        self._streams: dict[str, _StreamInfo] = {}
        self._watchers = tuple(watchers)
        self._event = asyncio.Event()

    def create_stream(
        self,
        stream_id: str,
        observer: StreamObserver | None,
        dtype: TypeHint[Any],
        labels: Mapping[str, str] | None,
        op_span: OpSpan | None,
    ) -> None:
        observers = [
            watcher
            for watcher, matcher in self._watchers
            if labels and all(labels.get(k) == v for k, v in matcher)
        ]
        if observer:
            observers.append(observer)

        self._streams[stream_id] = _StreamInfo(observers, dtype, labels, op_span)
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

    def get_info(self, stream_id: str) -> tuple[OpSpan | None] | None:
        if s := self._streams.get(stream_id):
            return (s.op_span,)
        return None

    def match_streams(self, matcher: Iterable[tuple[str, str]]) -> Sequence[str]:
        return tuple(
            stream_id
            for stream_id, info in self._streams.items()
            if info.labels and all(info.labels.get(k) == v for k, v in matcher)
        )

    async def wait_one(self, matcher: Iterable[tuple[str, str]]) -> str:
        while True:
            if match := self.match_streams(matcher):
                if len(match) != 1:
                    msg = "multiple streams matched"
                    raise RuntimeError(msg)
                return match[0]
            self._event.clear()
            await self._event.wait()
