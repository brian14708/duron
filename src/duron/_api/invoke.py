"""Run wiring and lifecycle: :class:`Invocation` and :func:`run`.

A workflow is *invoked* by calling it with arguments, producing an
:class:`Invocation` that carries those arguments plus the port wiring. The
invocation is inert until handed to :func:`run`. Await ``run(...)`` directly
for the result, or use it as an async context for imperative access.

Both forms own the session and spawn one pump per wiring. Cancelling the await
or leaving the context exceptionally stops local execution while keeping the
durable run resumable.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
from typing import TYPE_CHECKING, Generic, cast
from typing_extensions import Any, TypeVar

from duron._api.ports import Input
from duron._api.run import (
    Run,
    RunState,
    _NamedStreamScan,  # pyright: ignore[reportPrivateUsage]
)
from duron._core.session import CURRENT_VERSION, InitParams, Session, read_header
from duron.errors import HistoryMismatchError
from duron.log._helper import random_id
from duron.typing import UnspecifiedType, inspect_function

if TYPE_CHECKING:
    from collections.abc import (
        AsyncIterable,
        AsyncIterator,
        Awaitable,
        Callable,
        Generator,
    )
    from types import TracebackType

    from duron._api.ports import Output, Request, Signal
    from duron._api.run import InputWriter
    from duron._api.workflow import Workflow
    from duron._decorator.durable import DurableFn
    from duron.log import Storage
    from duron.tracing import Tracer
    from duron.typing import JSONValue

_T = TypeVar("_T")
_V = TypeVar("_V")
_Req = TypeVar("_Req")
_Res = TypeVar("_Res")


class Invocation(Generic[_T]):
    """A workflow bound to arguments and host-side port wiring.

    Build one by calling a :class:`~duron.Workflow`
    (``my_workflow(arg1, arg2)``), attach pumps with :meth:`feed`,
    :meth:`output`, and :meth:`serve`, then pass it to :func:`duron.run`. Each
    wiring method returns ``self`` so calls chain.

    An invocation is single-use: passing it to :func:`duron.run` consumes it
    (its ``feed`` sources are iterated once), so build a fresh invocation for
    each run attempt.
    """

    __slots__ = (
        "_args",
        "_feeds",
        "_kwargs",
        "_launched",
        "_outputs",
        "_serves",
        "_wf",
    )

    def __init__(
        self,
        workflow: Workflow[..., _T],
        args: tuple[object, ...],
        kwargs: dict[str, object],
    ) -> None:
        self._wf = workflow
        self._args = args
        self._kwargs = kwargs
        self._launched = False
        # Wiring recorded before the run starts; pumps are spawned in _supervise.
        self._feeds: list[tuple[Input[Any] | Signal[Any], AsyncIterable[Any]]] = []
        self._outputs: list[
            tuple[
                Output[Any],
                Callable[[AsyncIterator[tuple[int, Any]]], Awaitable[object]],
            ]
        ] = []
        self._serves: list[tuple[Request[Any, Any], Callable[[Any], object]]] = []

    def feed(
        self, port: Input[_V] | Signal[_V], source: AsyncIterable[_V]
    ) -> Invocation[_T]:
        """Send each value from ``source`` into a host→workflow ``port``.

        ``source`` is any async iterable; its values are sent in order. For an
        :class:`~duron.Input` port the writer is closed once the source is
        exhausted, signalling end-of-input to the workflow.

        Feeds are restart-safe: when a run is resumed, the source is
        re-iterated from the start, but values already recorded in the durable
        log are skipped (and an already-recorded close is not repeated), so
        re-attaching the same logical source never delivers a value twice.

        Returns:
            This invocation, for chaining.

        """
        self._feeds.append((port, source))
        return self

    def output(
        self,
        port: Output[_V],
        sink: Callable[[AsyncIterator[tuple[int, _V]]], Awaitable[object]],
    ) -> Invocation[_T]:
        """Await ``sink`` with an async iterator over ``port``'s emissions.

        ``sink`` receives a single async iterator yielding ``(offset, value)``
        pairs, where ``offset`` is the durable storage offset of each value;
        the iterator ends when the run reaches a terminal state. Registering
        the same port more than once attaches independent readers (fan-out),
        each with its own cursor.

        Returns:
            This invocation, for chaining.

        """
        self._outputs.append((port, sink))
        return self

    def serve(
        self,
        port: Request[_Req, _Res],
        handler: Callable[[_Req], Awaitable[_Res] | _Res],
    ) -> Invocation[_T]:
        """Answer each request on ``port`` with ``handler(request_value)``.

        ``handler`` receives the request payload and returns (or awaits to) the
        response; the pump resolves the pending request with that value.

        Returns:
            This invocation, for chaining.

        """
        self._serves.append((port, handler))
        return self

    async def _launch(self, session: Session) -> RunState[_T]:
        """Create the run in ``session``'s storage, or resume a matching one.

        An empty storage starts fresh; a persisted run whose workflow identity
        and inputs match is resumed.

        Returns:
            Internal run state used to supervise the invocation.

        Raises:
            RuntimeError: if this invocation was already passed to a run.
            HistoryMismatchError: if a persisted run's identity or inputs are
                incompatible.

        """
        if self._launched:
            msg = (
                "an Invocation is single-use; call the workflow again to build "
                "a fresh invocation for each run"
            )
            raise RuntimeError(msg)
        self._launched = True
        storage = session.storage
        codec = self._wf.codec
        header = await read_header(storage, codec)
        if header is None:
            return await self._spawn(session, resume=False)
        _validate_identity(self._wf, header)
        encoded_args, encoded_kwargs = _encode_inputs(
            self._wf.durable, self._args, dict(self._kwargs)
        )
        if header.get("args") != encoded_args or header.get("kwargs") != encoded_kwargs:
            msg = "inputs do not match the persisted run"
            raise HistoryMismatchError(
                msg,
                expected={"args": header.get("args"), "kwargs": header.get("kwargs")},
                actual={"args": encoded_args, "kwargs": encoded_kwargs},
            )
        return await self._spawn(session, resume=True)

    async def _spawn(self, session: Session, *, resume: bool) -> RunState[_T]:
        durable_fn = self._wf.durable
        nonce = random_id()

        if resume:

            def init() -> InitParams:
                msg = "Cannot initialize an already-started run"
                raise HistoryMismatchError(msg)

        else:
            encoded_args, encoded_kwargs = _encode_inputs(
                durable_fn, self._args, dict(self._kwargs)
            )
            # Bind only the small identity fields as locals so the closure does
            # not capture ``self`` (which would pin the whole Invocation and its
            # raw, un-encoded arguments for the run's lifetime).
            workflow_name = self._wf.name
            workflow_version = self._wf.version

            def init() -> InitParams:
                return cast(
                    "InitParams",
                    {
                        "version": CURRENT_VERSION,
                        "args": encoded_args,
                        "kwargs": encoded_kwargs,
                        "nonce": nonce,
                        "workflow_name": workflow_name,
                        "workflow_version": workflow_version,
                    },
                )

        task = session.spawn(durable_fn, init)
        await task.start()
        return RunState(task, session.storage, self._wf.codec)

    async def _start(self, session: Session) -> Run[_T]:
        """Launch the invocation in ``session`` and begin supervising it.

        Returns:
            The live run handle, whose result completes with supervision.

        """
        state = await self._launch(session)
        completion = asyncio.create_task(self._supervise(state))
        return Run(state, completion)

    async def _supervise(self, run: RunState[_T]) -> _T:
        """Supervise pumps against the run and return its result.

        Failure contract:

        * A pump raising cancels the other pumps and the workflow task, and the
          pump's error propagates from :func:`run`.
        * On normal workflow completion, ``output``/``serve`` drainers are left
          to finish reading buffered records (they self-terminate at the
          terminal state); ``feed`` pumps are cancelled.
        * External cancellation of :func:`run` stops local execution; the run
          stays resumable.

        Returns:
            The decoded workflow result.

        """
        result_task: asyncio.Task[_T] = asyncio.ensure_future(run.result())
        # Feeds share one log pass for their restart-safety prefix, so resuming
        # costs a single scan rather than one full rescan per feed.
        feed_state = await _recorded_feed_states(
            run.storage, [port.stream_name for port, _source in self._feeds]
        )
        drain_pumps: list[asyncio.Task[None]] = [
            asyncio.ensure_future(_pump_output(run, port, sink))
            for port, sink in self._outputs
        ] + [
            asyncio.ensure_future(_pump_serve(run, port, handler))
            for port, handler in self._serves
        ]
        feed_pumps: list[asyncio.Task[None]] = [
            asyncio.ensure_future(
                _pump_source(run, port, source, feed_state[port.stream_name])
            )
            for port, source in self._feeds
        ]
        pumps: list[asyncio.Task[None]] = [*drain_pumps, *feed_pumps]

        try:
            pending: set[asyncio.Future[Any]] = {result_task, *pumps}
            while pending:
                done, pending = await asyncio.wait(
                    pending, return_when=asyncio.FIRST_COMPLETED
                )
                # Success wins: if the workflow finished — or is terminal but
                # still running post-terminal cleanup, during which a feeder
                # racing loop teardown can fail a wait round before
                # result_task resolves — stop supervising and let the
                # workflow's own outcome decide.
                if result_task in done or run.is_terminal():
                    break
                for task in done:
                    if not task.cancelled():
                        exc = task.exception()
                        if exc is not None:
                            raise exc
            # Workflow is terminal: drainers self-terminate; feeders may be
            # unbounded, so cancel them.
            for task in feed_pumps:
                _ = task.cancel()
            _ = await asyncio.gather(*feed_pumps, return_exceptions=True)
            # Drainers are expected to finish after the terminal log tail is
            # consumed. Their delivery/handler errors remain run failures.
            _ = await asyncio.gather(*drain_pumps)
            return await result_task
        finally:
            for task in (result_task, *pumps):
                if not task.done():
                    _ = task.cancel()
            _ = await asyncio.gather(*pumps, return_exceptions=True)
            with contextlib.suppress(BaseException):
                _ = await result_task


async def _pump_source(
    run: RunState[Any],
    port: Input[Any] | Signal[Any],
    source: AsyncIterable[Any],
    recorded: tuple[int, bool],
) -> None:
    # Restart-safety: on a resumed run the source re-iterates from the start,
    # but values already recorded in the log are replayed to the workflow by
    # the engine. Re-sending them would deliver each twice (and a second close
    # would append a duplicate stream.complete), so skip exactly the durable
    # prefix: the log itself is the feed's checkpoint.
    skip, closed = recorded
    # Inputs are closed at end-of-source to signal end-of-input; signals are
    # notifications and are never closed. Only the sender acquisition differs,
    # so the skip/deliver loop is shared.
    sender = run.input(port) if isinstance(port, Input) else run.signal(port)
    async for value in source:
        if skip > 0:
            skip -= 1
            continue
        await sender.send(value)
    # The workflow may already be terminal (and the loop tearing down) by the
    # time we close; end-of-input is then moot, so swallow it.
    if isinstance(port, Input) and not closed:
        with contextlib.suppress(Exception):
            await cast("InputWriter[Any]", sender).close()


async def _recorded_feed_states(
    storage: Storage, stream_names: list[str]
) -> dict[str, tuple[int, bool]]:
    """Count each port's durable emits and whether its stream was closed.

    All requested names are resolved in a single log pass, so resuming with
    several feeds costs one scan rather than one full rescan per feed.

    Args:
        storage: The run's log storage.
        stream_names: The durable stream names to resolve.

    Returns:
        A ``(recorded_emit_count, closed)`` pair keyed by stream name, for every
        requested name.

    """
    counts: dict[str, int] = dict.fromkeys(stream_names, 0)
    closed: dict[str, bool] = dict.fromkeys(stream_names, False)
    if not counts:
        return dict.fromkeys(stream_names, (0, False))

    scan = _NamedStreamScan(storage, counts.keys())
    async for _offset, entry in scan.scan():
        if entry["type"] == "stream.emit":
            name = scan.stream_ids.get(entry["stream_id"])
            if name is not None:
                counts[name] += 1
        elif entry["type"] == "stream.complete":
            name = scan.stream_ids.get(entry["stream_id"])
            if name is not None:
                closed[name] = True
    return {name: (counts[name], closed[name]) for name in stream_names}


async def _resolve(fn: Callable[[Any], object], arg: object) -> object:
    """Call ``fn(arg)``, awaiting the result if the callable is async.

    Returns:
        The (awaited) return value of ``fn``.

    """
    result = fn(arg)
    return await result if inspect.isawaitable(result) else result


async def _pump_output(
    run: RunState[Any],
    port: Output[Any],
    sink: Callable[[AsyncIterator[tuple[int, Any]]], Awaitable[object]],
) -> None:
    reader = run.output(port)

    async def entries() -> AsyncIterator[tuple[int, Any]]:
        async for value in reader:
            yield reader.offset, value

    _ = await sink(entries())


async def _pump_serve(
    run: RunState[Any], port: Request[Any, Any], handler: Callable[[Any], object]
) -> None:
    async for pending in run.requests(port):
        await pending.respond(await _resolve(handler, pending.value))


class RunContext(Generic[_T]):
    """Awaitable async context returned by :func:`run`.

    Await it directly for the workflow result, or enter it to obtain a
    :class:`Run` for imperative port coordination. Single-use: it owns the
    session and the run's supervision for exactly one entry.
    """

    __slots__ = (
        "_completion",
        "_entered",
        "_invocation",
        "_session",
        "_storage",
        "_tracer",
    )

    def __init__(
        self, invocation: Invocation[_T], storage: Storage, tracer: Tracer | None
    ) -> None:
        self._invocation = invocation
        self._storage = storage
        self._tracer = tracer
        self._session: Session | None = None
        self._completion: asyncio.Task[_T] | None = None
        self._entered = False

    async def __aenter__(self) -> Run[_T]:
        if self._entered:
            msg = "a run context cannot be entered more than once"
            raise RuntimeError(msg)
        self._entered = True
        session = Session(self._storage, tracer=self._tracer)
        self._session = session
        await session.__aenter__()
        try:
            run = await self._invocation._start(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                session
            )
        except BaseException as exc:
            await session.__aexit__(type(exc), exc, exc.__traceback__)
            raise
        self._completion = run._completion  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        return run

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        session = self._session
        completion = self._completion
        if session is None or completion is None:
            return
        try:
            if exc_type is None:
                _ = await completion
            else:
                if not completion.done():
                    _ = completion.cancel()
                _ = await asyncio.gather(completion, return_exceptions=True)
        finally:
            await session.__aexit__(exc_type, exc_value, traceback)

    def __await__(self) -> Generator[Any, None, _T]:
        """Keep ``await run(...)`` as shorthand for awaiting the result.

        Returns:
            An iterator implementing the await protocol.

        """
        return self._result().__await__()

    async def _result(self) -> _T:
        async with self as run:
            return await run.result()


def run(
    invocation: Invocation[_T], storage: Storage, *, tracer: Tracer | None = None
) -> RunContext[_T]:
    """Execute ``invocation`` directly or create an imperative run context.

    Prefer ``await run(...)`` when declarative wiring is sufficient. For logic
    that coordinates across ports, ``async with run(...)`` yields a
    :class:`Run`. On clean context exit it waits for the workflow to finish. On
    exceptional exit it stops local execution; the durable run remains
    resumable.

    If ``storage`` is empty the run starts fresh. If it already holds a run
    whose workflow identity and inputs match, that run is resumed (a completed
    run simply returns its recorded result); a mismatch raises
    ``HistoryMismatchError``.

    Args:
        invocation: A workflow bound to arguments and port wiring.
        storage: The single-run log backend to execute against.
        tracer: Optional tracer for this run.

    Returns:
        A single-use object that can be awaited or entered as an async context.

    """
    return RunContext(invocation, storage, tracer)


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
    workflow: Workflow[Any, Any], header: dict[str, JSONValue]
) -> None:
    for field, persisted, requested in (
        ("name", header.get("workflow_name"), workflow.name),
        ("version", header.get("workflow_version"), workflow.version),
    ):
        if persisted is not None and persisted != requested:
            msg = (
                f"workflow {field} mismatch: persisted {persisted!r}, "
                f"requested {requested!r}"
            )
            raise HistoryMismatchError(msg, expected=persisted, actual=requested)
