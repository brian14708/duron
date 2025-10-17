from __future__ import annotations

from asyncio import CancelledError
from dataclasses import dataclass
from typing import TYPE_CHECKING
from typing_extensions import (
    Any,
    final,
)

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence

    from duron._core.ops import (
        StreamObserver,
    )
    from duron.codec import Codec
    from duron.tracing._tracer import OpSpan
    from duron.typing import JSONValue, TypeHint


@dataclass(slots=True)
class _StreamInfo:
    observers: Sequence[StreamObserver]
    dtype: TypeHint[Any]
    labels: Mapping[str, str]
    op_span: OpSpan | None


@final
class StreamManager:
    __slots__ = ("_streams", "_watchers")

    def __init__(
        self,
        watchers: list[tuple[dict[str, str], StreamObserver]] | None = None,
    ) -> None:
        self._streams: dict[
            str,
            _StreamInfo,
        ] = {}
        self._watchers = watchers or []

    def create_stream(
        self,
        stream_id: str,
        observer: StreamObserver | None,
        dtype: TypeHint[Any],
        labels: Mapping[str, str],
        op_span: OpSpan | None,
    ) -> None:
        observers = [
            watcher
            for matcher, watcher in self._watchers
            if _match_labels(labels, matcher)
        ]
        if observer:
            observers.append(observer)

        self._streams[stream_id] = _StreamInfo(
            observers,
            dtype,
            labels,
            op_span,
        )

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

        if isinstance(exc, CancelledError):
            exc = RuntimeError("stream closed", exc)
        for observer in info.observers:
            observer.on_close(offset, exc)
        return True

    def get_info(self, stream_id: str) -> tuple[OpSpan | None] | None:
        if s := self._streams.get(stream_id):
            return (s.op_span,)
        return None

    def find_matching_streams(
        self, matcher: Mapping[str, str]
    ) -> Iterator[tuple[str, OpSpan | None]]:
        return (
            (stream_id, info.op_span)
            for stream_id, info in self._streams.items()
            if info.labels and _match_labels(info.labels, matcher)
        )


def _match_labels(labels: Mapping[str, str], matcher: Mapping[str, str]) -> bool:
    return all(labels.get(k) == v for k, v in matcher.items())
