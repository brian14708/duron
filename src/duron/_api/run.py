"""Workflow execution, bound calls, and the host run lifecycle.

``Run`` is yielded by the `duron.open_run` async context manager. The context
owns its lifecycle while ``Run`` exposes imperative access to its result and
typed ports. ``WorkflowCall`` binds a workflow definition to its arguments
before the run starts.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, NoReturn, cast
from typing_extensions import TypeVar, override

from duron._api.ports import decode_request_envelope, utc_from_us
from duron._hints import UnspecifiedType
from duron._introspection import inspect_function
from duron._runtime.execution.errors import is_round_tripping_error_type
from duron._runtime.storage.helper import random_id
from duron._runtime.storage.pagination import iter_entries
from duron._runtime.workflow.session import (
    CURRENT_VERSION,
    InitParams,
    Session,
    read_header,
)
from duron.errors import (
    HistoryMismatchError,
    InvalidRunStateError,
    RequestAlreadyResolvedError,
    UndeclaredErrorTypeError,
)

if TYPE_CHECKING:
    from collections.abc import (
        AsyncGenerator,
        AsyncIterator,
        Awaitable,
        Callable,
        Collection,
    )
    from datetime import datetime

    from duron._api.ports import Input, Interrupt, Output, Request
    from duron._runtime.decorator import DurableFn
    from duron._runtime.storage.entry import Entry
    from duron._runtime.workflow.session import Task
    from duron._runtime.workflow.stream import StreamWriter
    from duron.codec import Codec
    from duron.storage import HistoryStore
    from duron.types import JSONValue

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


@dataclass(frozen=True, slots=True)
class _RunState(Generic[_T]):
    """Resources owned by a live run, without a second run API."""

    task: Task[_T]
    backend: HistoryStore
    codec: Codec
    run_id: str
    seed: str


@dataclass(frozen=True, slots=True)
class OutputCursor:
    """Position in one run's named output stream."""

    run_id: str
    stream_name: str
    offset: int

    def __post_init__(self) -> None:
        if type(self.offset) is not int or self.offset < 0:
            msg = "output cursor offset must be a non-negative integer"
            raise ValueError(msg)


class Run(Generic[_T]):
    """Imperative access to a live workflow and its typed ports.

    Instances are yielded by ``async with duron.open_run(...)``. The surrounding
    context owns execution and cleanup; this handle can await the workflow
    result and coordinate directly across any of its ports.
    """

    __slots__ = ("_completion", "_state")

    def __init__(self, state: _RunState[_T]) -> None:
        self._state = state
        self._completion = asyncio.ensure_future(state.task.result())

    @property
    def backend(self) -> HistoryStore:
        """The backend containing this run's committed log."""
        return self._state.backend

    @property
    def run_id(self) -> str:
        """The selected run within this backend."""
        return self._state.run_id

    async def result(self) -> _T:
        """Wait for workflow completion.

        Returns:
            The decoded workflow result.

        """
        return await asyncio.shield(self._completion)

    def output(
        self, port: Output[_V], *, after: OutputCursor | None = None
    ) -> OutputReader[_V]:
        """Read values from an output port.

        Args:
            port: The output port to read.
            after: Resume strictly after a cursor from this run and port.
                The default reads from the beginning.

        Returns:
            A reader over the port's durable event log.

        """
        return OutputReader(
            self._state.backend,
            self._state.codec,
            port,
            self._state.task.is_terminal,
            wait=self._state.task.wait_for_append,
            after=after,
            run_id=self._state.run_id,
        )

    def input(self, port: Input[_V]) -> InputWriter[_V]:
        """Open a host writer for FIFO input.

        Returns:
            A writer whose repeated sends enqueue new values.

        """
        return InputWriter(self._state.task, port.stream_name)

    def interrupt(self, port: Interrupt[_V]) -> InterruptSender[_V]:
        """Open a sender for an interrupt port.

        Returns:
            A sender for notifications to an armed interruptible block.

        """
        return InterruptSender(self._state.task, port.stream_name)

    def requests(self, port: Request[_Req, _Res]) -> RequestReader[_Req, _Res]:
        """Read unresolved requests, including those pending before recovery.

        Returns:
            A reader that yields each unresolved request once per reader.

        """
        return RequestReader(
            self._state.backend,
            self._state.task,
            self._state.codec,
            port,
            wait=self._state.task.wait_for_append,
            run_id=self._state.run_id,
        )


class _PollingReader(AsyncIterable[_V], Generic[_V]):
    """Base for readers that poll a durable log until the run is terminal.

    Subclasses implement `_iterate` as an async generator; this base
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
        justifies another backend pass.
        """
        while not await self._idle():
            pass

    @override
    def __aiter__(self) -> AsyncIterator[_V]:
        return self._cursor()

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

        Shares the cursor with `next` and async iteration, returning only
        values not yet consumed. Create another reader for independent reads.

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
    consumed internally; every other entry is yielded. When ``skip_through``
    is given, entries at or below that offset are consumed (so stream
    creations still register) but not yielded.
    """

    __slots__ = (
        "_backend",
        "_names",
        "_run_id",
        "_skip_through",
        "scanned",
        "stream_ids",
    )

    def __init__(
        self,
        backend: HistoryStore,
        names: Collection[str],
        skip_through: int = 0,
        *,
        run_id: str,
    ) -> None:
        self._backend = backend
        self._run_id = run_id
        self._names = set(names)
        self._skip_through = skip_through
        self.scanned: int | None = None
        self.stream_ids: dict[str, str] = {}

    async def scan(self) -> AsyncIterator[tuple[int, Entry]]:
        async for offset, entry in iter_entries(
            self._backend, self._run_id, after=self.scanned
        ):
            self.scanned = offset
            if entry["type"] == "stream.create":
                name = entry["name"]
                if name is not None and name in self._names:
                    self.stream_ids[entry["id"]] = name
                continue
            if offset <= self._skip_through:
                continue
            yield offset, entry


class OutputReader(_PollingReader[_T]):
    """A cursor-based reader over a durable output port's event log."""

    __slots__ = (
        "_backend",
        "_codec",
        "_offset",
        "_port",
        "_run_id",
        "_start_offset",
        "_terminal",
    )

    def __init__(
        self,
        backend: HistoryStore,
        codec: Codec,
        port: Output[_T],
        terminal: Callable[[], bool],
        wait: Callable[[int], Awaitable[int]] | None = None,
        *,
        after: OutputCursor | None = None,
        run_id: str,
    ) -> None:
        super().__init__(wait)
        self._backend = backend
        self._run_id = run_id
        self._codec = codec
        self._port = port
        self._terminal = terminal
        if after is not None and (
            not isinstance(cast("object", after), OutputCursor)
            or after.run_id != run_id
            or after.stream_name != port.stream_name
        ):
            msg = "output cursor belongs to a different run or stream"
            raise ValueError(msg)
        self._offset = after.offset if after is not None else 0
        self._start_offset = self._offset

    @property
    def cursor(self) -> OutputCursor:
        """The position of the last value delivered by this reader."""
        return OutputCursor(self._run_id, self._port.stream_name, self._offset)

    @override
    async def _iterate(self) -> AsyncIterator[_T]:
        scan = _NamedStreamScan(
            self._backend,
            {self._port.stream_name},
            skip_through=self._start_offset,
            run_id=self._run_id,
        )
        closed = False
        terminal_seen = False
        while True:
            progressed = False
            async for offset, entry in scan.scan():
                if (
                    entry["type"] == "stream.emit"
                    and entry["stream_id"] in scan.stream_ids
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

    __slots__ = ("_lock", "_name", "_task", "_writer")

    def __init__(self, task: Task[Any], name: str) -> None:
        self._task = task
        self._name = name
        self._writer: StreamWriter[_T] | None = None
        self._lock = asyncio.Lock()

    async def _resolve(self) -> StreamWriter[_T]:
        writer = self._writer
        if writer is None:
            async with self._lock:
                writer = self._writer
                if writer is None:
                    writer = cast(
                        "StreamWriter[_T]", await self._task.open_stream(self._name)
                    )
                    self._writer = writer
        return writer

    async def send(self, value: _T, /) -> None:
        """Append a value durably. Each call sends a new value without deduplication."""
        writer = await self._resolve()
        await writer.send(value)


class InputWriter(_LazyWriter[_T]):
    """Sends values into a workflow input port."""

    __slots__ = ()

    async def close(self) -> None:
        """Record end of input; buffered values remain available to the workflow."""
        writer = await self._resolve()
        await writer.close()


class InterruptSender(_LazyWriter[_T]):
    """Sends a notification into a workflow interrupt port.

    Unlike `InputWriter` there is no ``close()``: a workflow arms an
    interrupt to wait for a single notification, never for "no more
    interrupts", so closing the stream would have no consumer-visible meaning.
    """

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

        """  # ruff: ignore[docstring-extraneous-exception]
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

        """  # ruff: ignore[docstring-extraneous-exception]
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

    @property
    def is_pending(self) -> bool:
        """Whether the request is still awaiting a reply."""
        return self._task.is_future_pending(self.id)


class RequestReader(_PollingReader["PendingRequest[_Req, _Res]"], Generic[_Req, _Res]):
    """Yields each unresolved `PendingRequest` as it appears in history."""

    __slots__ = ("_backend", "_codec", "_port", "_run_id", "_task")

    def __init__(
        self,
        backend: HistoryStore,
        task: Task[Any],
        codec: Codec,
        port: Request[_Req, _Res],
        wait: Callable[[int], Awaitable[int]] | None = None,
        *,
        run_id: str,
    ) -> None:
        super().__init__(wait)
        self._backend = backend
        self._run_id = run_id
        self._task = task
        self._codec = codec
        self._port = port

    @override
    async def _iterate(self) -> AsyncIterator[PendingRequest[_Req, _Res]]:
        # Both sets are pruned as requests resolve (each emit/complete entry is
        # scanned exactly once thanks to the offset cursor), so retention stays
        # bounded by the number of currently open requests rather than growing
        # by one id per request for the life of a served workflow.
        scan = _NamedStreamScan(
            self._backend, {self._port.stream_name}, run_id=self._run_id
        )
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
            if self._task.is_terminal() and not progressed:
                return
            if not progressed:
                await self._wait_for_change()


class WorkflowCall(Generic[_T]):
    """A single-use workflow call; bind a fresh call for each run or resume."""

    __slots__ = (
        "_args",
        "_codec",
        "_codec_id",
        "_durable",
        "_kwargs",
        "_launched",
        "_name",
        "_version",
    )

    def __init__(
        self,
        durable: DurableFn[..., _T],
        args: tuple[object, ...],
        kwargs: dict[str, object],
        *,
        name: str,
        version: str,
        codec: Codec,
        codec_id: str,
    ) -> None:
        self._durable = durable
        self._name = name
        self._version = version
        self._codec = codec
        self._codec_id = codec_id
        self._args = args
        self._kwargs = kwargs
        self._launched = False

    async def _launch(self, session: Session) -> Run[_T]:
        if self._launched:
            msg = "a WorkflowCall is single-use; call the workflow again for each run"
            raise InvalidRunStateError(msg)
        self._launched = True
        header = await read_header(session.backend, self._codec, run_id=session.run_id)
        encoded_args, encoded_kwargs = _encode_inputs(
            self._durable, self._args, dict(self._kwargs)
        )
        if header is not None:
            _validate_identity(
                header, name=self._name, version=self._version, codec_id=self._codec_id
            )
            if (
                header.get("args") != encoded_args
                or header.get("kwargs") != encoded_kwargs
            ):
                msg = "inputs do not match the persisted run"
                raise HistoryMismatchError(
                    msg,
                    expected={
                        "args": header.get("args"),
                        "kwargs": header.get("kwargs"),
                    },
                    actual={"args": encoded_args, "kwargs": encoded_kwargs},
                )
        nonce = random_id() if header is None else cast("str", header["nonce"])

        def init() -> InitParams:
            if header is not None:
                msg = "Cannot initialize an already-started run"
                raise HistoryMismatchError(msg)
            return cast(
                "InitParams",
                {
                    "version": CURRENT_VERSION,
                    "args": encoded_args,
                    "kwargs": encoded_kwargs,
                    "nonce": nonce,
                    "workflow_name": self._name,
                    "workflow_version": self._version,
                    "workflow_codec_id": self._codec_id,
                },
            )

        task = session.spawn(self._durable, init)
        await task.start()
        return Run(_RunState(task, session.backend, self._codec, session.run_id, nonce))


@contextlib.asynccontextmanager
async def open_run(
    call: WorkflowCall[_T], store: HistoryStore, *, run_id: str
) -> AsyncGenerator[Run[_T], None]:
    """Create or resume a run for host interaction through typed ports.

    Normal context exit waits for the workflow result and propagates its
    failure. Exceptional exit or host cancellation cleans up local execution
    and releases the lease, preserving committed history for resumption.
    Cancelling a waiter on `Run.result` alone does not stop execution.

    Supply a fresh bound call each time, with the same definition identity and
    normalized inputs to resume an existing `run_id`.

    Yields:
        The live run handle.

    """
    async with Session(store, run_id=run_id) as session:
        active = await call._launch(session)  # pyright: ignore[reportPrivateUsage]  # ruff: ignore[private-member-access]
        try:
            yield active
            _ = await active.result()
        finally:
            completion = active._completion  # pyright: ignore[reportPrivateUsage]  # ruff: ignore[private-member-access]
            if not completion.done():
                _ = completion.cancel()
            _ = await asyncio.gather(completion, return_exceptions=True)


async def run(call: WorkflowCall[_T], store: HistoryStore, *, run_id: str) -> _T:
    """Create or resume a run and return its final result.

    Uses the same lifecycle as `open_run`. The application must reopen the run
    after a process restart; Duron does not start a replacement process.

    Returns:
        The decoded workflow result.

    """
    async with open_run(call, store, run_id=run_id) as active:
        return await active.result()


def _encode_inputs(
    fn: DurableFn[..., Any], args: tuple[object, ...], kwargs: dict[str, object]
) -> tuple[list[JSONValue], dict[str, JSONValue]]:
    hint = inspect_function(fn.fn)
    codec = fn.codec
    encoded_args = [
        codec.encode_json(arg, fn.positional_type(i)) for i, arg in enumerate(args)
    ]
    encoded_kwargs = {
        key: codec.encode_json(value, hint.parameter_types.get(key, UnspecifiedType))
        for key, value in kwargs.items()
    }
    return encoded_args, encoded_kwargs


def _validate_identity(
    header: dict[str, JSONValue], *, name: str, version: str, codec_id: str
) -> None:
    for field, persisted, requested in (
        ("name", header.get("workflow_name"), name),
        ("version", header.get("workflow_version"), version),
        ("codec", header.get("workflow_codec_id"), codec_id),
    ):
        if persisted != requested:
            msg = (
                f"workflow {field} mismatch: persisted {persisted!r}, "
                f"requested {requested!r}"
            )
            raise HistoryMismatchError(msg, expected=persisted, actual=requested)
