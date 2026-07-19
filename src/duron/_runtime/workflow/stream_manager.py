from __future__ import annotations

import asyncio
from asyncio import CancelledError
from typing import TYPE_CHECKING, Any, final
from typing_extensions import NamedTuple

from duron.errors import InvalidRunStateError

if TYPE_CHECKING:
    from duron._runtime.workflow.ops import StreamObserver
    from duron.codec import Codec
    from duron.types import JSONValue, TypeHint


class _StreamInfo(NamedTuple):
    observer: StreamObserver | None
    dtype: TypeHint[Any]
    name: str | None
    error_types: tuple[type[BaseException], ...]


@final
class StreamManager:
    __slots__ = ("_closed", "_event", "_name_ids", "_streams")

    def __init__(self) -> None:
        self._streams: dict[str, _StreamInfo] = {}
        self._name_ids: dict[str, set[str]] = {}
        self._event = asyncio.Event()
        self._closed = False

    def create_stream(
        self,
        stream_id: str,
        observer: StreamObserver | None,
        dtype: TypeHint[Any],
        name: str | None,
        error_types: tuple[type[BaseException], ...] = (),
    ) -> None:
        self._streams[stream_id] = _StreamInfo(observer, dtype, name, error_types)

    def commit_stream(self, stream_id: str) -> None:
        info = self._streams.get(stream_id)
        if info is not None and info.name is not None:
            self._name_ids.setdefault(info.name, set()).add(stream_id)
        self._event.set()

    def send_to_stream(
        self, stream_id: str, codec: Codec, offset: int, value: JSONValue
    ) -> bool:
        info = self._streams.get(stream_id)
        if not info:
            return False
        if info.observer:
            info.observer.on_next(offset, codec.decode_json(value, info.dtype))
        return True

    def close_stream(
        self, stream_id: str, offset: int, exc: Exception | CancelledError | None
    ) -> bool:
        info = self._streams.pop(stream_id, None)
        if not info:
            return False
        if info.name is not None:
            ids = self._name_ids.get(info.name)
            if ids is not None:
                ids.discard(stream_id)
                if not ids:
                    self._name_ids.pop(info.name, None)
        self._event.set()

        if isinstance(exc, CancelledError):
            exc = RuntimeError("stream closed", exc)
        if info.observer:
            info.observer.on_close(offset, exc)
        return True

    def get_info(self, stream_id: str) -> tuple[TypeHint[Any]] | None:
        if s := self._streams.get(stream_id):
            return (s.dtype,)
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
            ids = self._name_ids.get(name)
            if ids:
                if len(ids) != 1:
                    msg = "multiple streams matched"
                    raise RuntimeError(msg)
                return next(iter(ids))
            if self._closed:
                msg = (
                    f"stream {name!r} was never created before the run reached a "
                    "terminal state"
                )
                raise InvalidRunStateError(msg)
            self._event.clear()
            await self._event.wait()
