"""Live run access and typed host-side port readers/writers.

``Run`` is yielded by the :func:`duron.run` async context manager. The context
owns its lifecycle while ``Run`` exposes imperative access to its result and
typed ports.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterable
from typing import TYPE_CHECKING, Generic, NoReturn, cast
from typing_extensions import Any, TypeVar, override

from duron._api.ports import decode_request_envelope, utc_from_us
from duron._core.utils import is_round_tripping_error_type
from duron.errors import (
    InvalidRunStateError,
    RequestAlreadyResolvedError,
    UndeclaredErrorTypeError,
)
from duron.log._helper import is_entry

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Collection
    from datetime import datetime

    from duron._api.ports import Input, Output, Request, Signal
    from duron._core.session import Task
    from duron._core.stream import StreamWriter
    from duron.codec import Codec
    from duron.log import Storage
    from duron.log._entry import Entry

_T = TypeVar("_T")
_Req = TypeVar("_Req")
_Res = TypeVar("_Res")
_V = TypeVar("_V")


# Fallback sleep when no change-notifier is wired, and a safety cap on the
# notified wait so a missed wakeup self-heals rather than hanging. The notifier
# fires on every append and on terminal state, so the cap should never actually
# be hit; it is kept generous because reaching it means allocating a fresh task
# and future per reader, which is work we do not want to repeat every second for
# the lifetime of an idle reader.
_POLL_INTERVAL = 0.005
_IDLE_TIMEOUT = 30.0


class RunState(Generic[_T]):
    """Internal state for a live workflow."""

    __slots__ = ("_codec", "_storage", "_task")

    def __init__(self, task: Task[_T], storage: Storage, codec: Codec) -> None:
        self._task = task
        self._storage = storage
        self._codec = codec

    def is_terminal(self) -> bool:
        return self._task.is_terminal()

    @property
    def storage(self) -> Storage:
        """The log storage backing this run."""
        return self._storage

    async def result(self) -> _T:
        """Wait for workflow completion.

        Returns:
            The decoded workflow result.

        """
        return await self._task.result()

    def output(self, port: Output[_V]) -> OutputReader[_V]:
        return OutputReader(
            self._storage,
            self._codec,
            port,
            self.is_terminal,
            wait=self._task.wait_for_append,
        )

    def input(self, port: Input[_V]) -> InputWriter[_V]:
        return InputWriter(self._task, port.stream_name)

    def signal(self, port: Signal[_V]) -> SignalSender[_V]:
        return SignalSender(self._task, port.stream_name)

    def requests(self, port: Request[_Req, _Res]) -> RequestReader[_Req, _Res]:
        return RequestReader(
            self._storage,
            self._task,
            self._codec,
            port,
            self.is_terminal,
            wait=self._task.wait_for_append,
        )


class Run(Generic[_T]):
    """Imperative access to a live workflow and its typed ports.

    Instances are yielded by ``async with duron.run(...)``. The surrounding
    context owns execution and cleanup; this handle can await the workflow
    result and coordinate directly across any of its ports.
    """

    __slots__ = ("_completion", "_state")

    def __init__(self, state: RunState[_T], completion: asyncio.Task[_T]) -> None:
        self._state = state
        self._completion = completion

    async def result(self) -> _T:
        """Wait for completion, including all configured port pumps.

        Returns:
            The decoded workflow result.

        """
        return await asyncio.shield(self._completion)

    def output(self, port: Output[_V]) -> OutputReader[_V]:
        return self._state.output(port)

    def input(self, port: Input[_V]) -> InputWriter[_V]:
        return self._state.input(port)

    def signal(self, port: Signal[_V]) -> SignalSender[_V]:
        return self._state.signal(port)

    def requests(self, port: Request[_Req, _Res]) -> RequestReader[_Req, _Res]:
        return self._state.requests(port)


class _PollingReader(AsyncIterable[_V], Generic[_V]):
    """Base for readers that poll a durable log until the run is terminal.

    Subclasses implement :meth:`_iterate` as an async generator; this base
    supplies the shared cursor slot and the iteration helpers.
    """

    __slots__ = ("_it", "_wait", "_wait_version")

    def __init__(self, wait: Callable[[int], Awaitable[int]] | None = None) -> None:
        self._it: AsyncIterator[_V] | None = None
        self._wait = wait
        self._wait_version = 0

    def _iterate(self) -> AsyncIterator[_V]:
        raise NotImplementedError

    async def _idle(self) -> bool:
        """Wait for new log activity, or fall back to a short sleep.

        Uses the run's append notifier when available so the reader wakes on the
        next durable append (or completion) instead of busy-polling; a bounded
        timeout self-heals against any missed wakeup.

        Returns:
            True if log activity may have occurred, False if the wait timed
            out untouched (in which case a rescan is provably empty).

        """
        if self._wait is None:
            await asyncio.sleep(_POLL_INTERVAL)
            return True
        # Owning the task avoids Python 3.11 wait_for cancellation races that
        # can strand the notifier while a reader pump is being torn down.
        wait_task = asyncio.ensure_future(self._wait(self._wait_version))
        try:
            done, _pending = await asyncio.wait((wait_task,), timeout=_IDLE_TIMEOUT)
            if not done:
                return False
            self._wait_version = wait_task.result()
            return True
        finally:
            if not wait_task.done():
                _ = wait_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                _ = await wait_task

    async def _wait_for_change(self) -> None:
        """Wait until the log may have changed, skipping empty rescans.

        A notifier timeout proves no entries were appended (and the run did
        not finish, which also advances the version), so only a real wakeup
        justifies another storage pass.
        """
        while not await self._idle():
            pass

    @override
    def __aiter__(self) -> AsyncIterator[_V]:
        return self._iterate()

    def _cursor(self) -> AsyncIterator[_V]:
        """Return the shared iteration cursor, starting it on first use.

        Returns:
            The shared cursor, created on first call.

        """
        if self._it is None:
            self._it = self._iterate()
        return self._it

    async def next(self) -> _V:
        """Return the next value, advancing a shared cursor across calls.

        Returns:
            The next value to appear on the port.

        Raises:
            InvalidRunStateError: if the run reaches a terminal state before
                another value appears.

        """
        try:
            return await anext(self._cursor())
        except StopAsyncIteration:
            msg = "run reached a terminal state before the next value appeared"
            raise InvalidRunStateError(msg) from None

    async def collect(self) -> list[_V]:
        """Collect every remaining value up to the terminal state.

        Shares the cursor with :meth:`next`, so after some values have been
        consumed via ``next()`` this returns only those not yet seen.

        Returns:
            All not-yet-consumed values, up to the current terminal state.

        """
        return [item async for item in self._cursor()]


class _NamedStreamScan:
    """Incremental log scan tracking the streams created for a set of names.

    ``stream_ids`` maps each created stream id back to the name that created it,
    so one scan can serve readers that each watch a single name as well as
    reducers that aggregate several names in a single pass.

    The offset cursor resumes each pass where the last one ended, so a poll
    costs O(new entries), not O(history). ``stream.create`` records are
    consumed internally; every other entry is yielded.
    """

    __slots__ = ("_names", "_storage", "scanned", "stream_ids")

    def __init__(self, storage: Storage, names: Collection[str]) -> None:
        self._storage = storage
        self._names = set(names)
        self.scanned: int | None = None
        self.stream_ids: dict[str, str] = {}

    async def scan(self) -> AsyncIterator[tuple[int, Entry]]:
        async for offset, entry in self._storage.stream(offset=self.scanned):
            self.scanned = offset
            if not is_entry(entry):
                continue
            if entry["type"] == "stream.create":
                name = entry["name"]
                if name is not None and name in self._names:
                    self.stream_ids[entry["id"]] = name
                continue
            yield offset, entry


class OutputReader(_PollingReader[_T]):
    """A cursor-based reader over a durable output port's event log."""

    __slots__ = ("_codec", "_offset", "_port", "_storage", "_terminal")

    def __init__(
        self,
        storage: Storage,
        codec: Codec,
        port: Output[_T],
        terminal: Callable[[], bool],
        wait: Callable[[int], Awaitable[int]] | None = None,
    ) -> None:
        super().__init__(wait)
        self._storage = storage
        self._codec = codec
        self._port = port
        self._terminal = terminal
        self._offset = 0

    @property
    def offset(self) -> int:
        """The storage offset of the last value delivered by this reader."""
        return self._offset

    @override
    async def _iterate(self) -> AsyncIterator[_T]:
        scan = _NamedStreamScan(self._storage, {self._port.stream_name})
        closed = False
        terminal_seen = False
        while True:
            progressed = False
            async for offset, entry in scan.scan():
                if entry["type"] == "stream.emit" and entry["stream_id"] in (
                    scan.stream_ids
                ):
                    progressed = True
                    value = cast(
                        "_T",
                        self._codec.decode_json(entry["value"], self._port.item_type),
                    )
                    self._offset = offset
                    yield value
                elif (
                    entry["type"] == "stream.complete"
                    and entry["stream_id"] in scan.stream_ids
                ):
                    closed = True
            if closed:
                return
            if self._terminal():
                # A backend may snapshot its tail before yielding. The run can
                # append its final output and become terminal during that scan,
                # so require one further scan after first observing terminal.
                if terminal_seen and not progressed:
                    return
                terminal_seen = True
                continue
            terminal_seen = False
            if not progressed:
                await self._wait_for_change()


class _LazyWriter(Generic[_T]):
    """Lazily opens and caches a write stream for a named port."""

    __slots__ = ("_name", "_task", "_writer")

    def __init__(self, task: Task[Any], name: str) -> None:
        self._task = task
        self._name = name
        self._writer: StreamWriter[_T] | None = None

    async def _resolve(self) -> StreamWriter[_T]:
        if self._writer is None:
            self._writer = cast(
                "StreamWriter[_T]", await self._task.open_stream(self._name)
            )
        return self._writer

    async def send(self, value: _T, /) -> None:
        writer = await self._resolve()
        await writer.send(value)


class InputWriter(_LazyWriter[_T]):
    """Sends values into a workflow input port."""

    __slots__ = ()

    async def close(self) -> None:
        writer = await self._resolve()
        await writer.close()


class SignalSender(_LazyWriter[_T]):
    """Sends a signal into a workflow signal port."""

    __slots__ = ()


class PendingRequest(Generic[_Req, _Res]):
    """An unresolved durable request awaiting a reply."""

    __slots__ = ("_codec", "_port", "_task", "created_at", "id", "value")

    def __init__(
        self,
        request_id: str,
        value: _Req,
        created_at: datetime,
        task: Task[Any],
        port: Request[_Req, _Res],
        codec: Codec,
    ) -> None:
        self.id = request_id
        self.value = value
        self.created_at = created_at
        self._task = task
        self._port = port
        self._codec = codec

    async def respond(self, value: _Res, /) -> None:
        """Resolve the request with a value.

        A request may be resolved only once; a second call (with any value)
        raises rather than being ignored.

        Raises:
            RequestAlreadyResolvedError: if the request has already been
                resolved.

        """  # noqa: DOC502
        self._ensure_pending()
        await self._task.complete_future(
            self.id, result=value, result_type=self._port.response_type
        )

    async def fail(self, error: Exception, /) -> None:
        """Fail the request with a declared exception type.

        Raises:
            UndeclaredErrorTypeError: if the exception type is neither declared
                in the port's ``raises`` nor a built-in exception.
            RequestAlreadyResolvedError: if the request has already been
                resolved.

        """  # noqa: DOC502
        error_type = type(error)
        if not is_round_tripping_error_type(error_type, self._port.raises):
            msg = (
                f"fail() requires a declared exception type; {error_type.__name__!r}"
                f" is not declared in Request({self._port.name!r}, raises=[...])"
            )
            raise UndeclaredErrorTypeError(msg)
        self._ensure_pending()
        await self._task.complete_future(self.id, exception=error)

    def _ensure_pending(self) -> None:
        if not self._task.is_future_pending(self.id):
            self._raise_already_resolved()

    def _raise_already_resolved(self) -> NoReturn:
        # The pre-check can lose a concurrent resolution race: complete_future
        # re-checks under its lock and raises RequestAlreadyResolvedError if
        # the future was resolved after the pre-check. Both paths surface the
        # same documented error.
        msg = f"request {self.id!r} is already resolved"
        raise RequestAlreadyResolvedError(msg)

    async def is_pending(self) -> bool:
        """Return whether the request is still awaiting a reply.

        Returns:
            ``True`` while the request remains unresolved.

        """
        return self._task.is_future_pending(self.id)


class RequestReader(_PollingReader["PendingRequest[_Req, _Res]"], Generic[_Req, _Res]):
    """Yields each unresolved :class:`PendingRequest` as it appears in history."""

    __slots__ = ("_codec", "_port", "_storage", "_task", "_terminal")

    def __init__(
        self,
        storage: Storage,
        task: Task[Any],
        codec: Codec,
        port: Request[_Req, _Res],
        terminal: Callable[[], bool],
        wait: Callable[[int], Awaitable[int]] | None = None,
    ) -> None:
        super().__init__(wait)
        self._storage = storage
        self._task = task
        self._codec = codec
        self._port = port
        self._terminal = terminal

    @override
    async def _iterate(self) -> AsyncIterator[PendingRequest[_Req, _Res]]:
        # Both sets are pruned as requests resolve (each emit/complete entry is
        # scanned exactly once thanks to the offset cursor), so retention stays
        # bounded by the number of currently open requests rather than growing
        # by one id per request for the life of a served workflow.
        scan = _NamedStreamScan(self._storage, {self._port.stream_name})
        seen: set[str] = set()
        resolved: set[str] = set()
        while True:
            progressed = False
            envelopes: list[tuple[str, Any, int]] = []
            pass_ids: set[str] = set()
            async for _offset, entry in scan.scan():
                if entry["type"] == "stream.emit":
                    if entry["stream_id"] not in scan.stream_ids:
                        continue
                    pair = decode_request_envelope(self._codec, entry["value"])
                    if pair is not None:
                        request_id, encoded = pair
                        envelopes.append((request_id, encoded, entry["ts"]))
                        pass_ids.add(request_id)
                elif entry["type"] == "promise.complete":
                    promise_id = entry["promise_id"]
                    if promise_id in seen:
                        # Surfaced earlier and now resolved; forget it.
                        seen.discard(promise_id)
                    elif promise_id in pass_ids:
                        # Resolved in the same pass its envelope appeared;
                        # remembered just long enough to skip the yield below.
                        # Completions of other promises (e.g. effect calls)
                        # are irrelevant here and deliberately not tracked.
                        resolved.add(promise_id)
            for request_id, encoded, ts in envelopes:
                if request_id in resolved:
                    resolved.discard(request_id)
                    continue
                if request_id in seen:
                    continue
                seen.add(request_id)
                progressed = True
                yield PendingRequest(
                    request_id,
                    cast(
                        "_Req",
                        self._codec.decode_json(encoded, self._port.request_type),
                    ),
                    utc_from_us(ts),
                    self._task,
                    self._port,
                    self._codec,
                )
            if self._terminal() and not progressed:
                return
            if not progressed:
                await self._wait_for_change()
