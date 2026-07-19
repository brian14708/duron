"""Workflow-facing context: the single object workflow code interacts with."""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections import deque
from collections.abc import AsyncIterable
from typing import TYPE_CHECKING, Generic, cast
from typing_extensions import Any, ParamSpec, TypeVar, override

from duron._api.effect import item_type_of
from duron._api.ports import encode_request_envelope, utc_from_us
from duron._core.signal import SignalInterrupt
from duron._core.stream import StreamClosed, StreamWriter
from duron.errors import (
    Interrupted,
    InvalidRunStateError,
    PortClosedError,
    WorkflowDefinitionError,
)

if TYPE_CHECKING:
    import random
    from collections.abc import AsyncGenerator, AsyncIterator, Callable, Coroutine
    from contextlib import AbstractAsyncContextManager
    from datetime import datetime

    from duron._api.effect import Effect
    from duron._api.ports import Input, Output, Request, Signal
    from duron._core.context import Context
    from duron._core.signal import Signal as _CoreSignal
    from duron._core.stream import Stream
    from duron.codec import Codec
    from duron.typing import TypeHint

_T = TypeVar("_T")
_S = TypeVar("_S")
_Req = TypeVar("_Req")
_Res = TypeVar("_Res")
_P = ParamSpec("_P")


class StreamCall(AsyncIterable[_S], Generic[_S]):
    """A durable streaming-effect call.

    Iterate it to observe each durably-recorded item; :meth:`result` returns the
    number of items the underlying generator yielded.
    """

    __slots__ = (
        "_args",
        "_ctx",
        "_effect",
        "_exhausted",
        "_failure_observed",
        "_kwargs",
        "_reader",
        "_task",
    )

    def __init__(
        self,
        ctx: Context,
        effect: Effect[..., Any],
        args: tuple[object, ...],
        kwargs: dict[str, object],
    ) -> None:
        self._ctx = ctx
        self._effect = effect
        self._args = args
        self._kwargs = kwargs
        self._reader: Stream[_S] | None = None
        self._task: asyncio.Task[int] | None = None
        self._exhausted = False
        self._failure_observed = False

    async def __aenter__(self) -> StreamCall[_S]:
        item_t = item_type_of(self._effect)
        effect_name = self._effect.name
        metadata = self._effect.op_metadata
        reader, writer = await self._ctx.create_stream(
            cast("TypeHint[_S]", item_t),
            name=effect_name,
            metadata=metadata,
            # Opt into replay adoption: if a crash interrupts the worker, its
            # already-recorded emits replay to the reader and the re-running
            # worker skips re-recording them instead of duplicating.
            adopt_replayed=True,
        )
        self._reader = reader
        effect = self._effect
        args = self._args
        kwargs = self._kwargs

        async def worker() -> int:
            count = 0
            async with writer as sink:
                async for item in effect.fn(*args, **kwargs):
                    await sink.send(item)
                    count += 1
            return count

        self._task = asyncio.ensure_future(
            self._ctx.run_effect(metadata, worker, return_type=int)
        )
        return self

    async def __aexit__(
        self, exc_type: object, exc_value: object, _traceback: object
    ) -> None:
        task = self._task
        if task is None:
            return

        if (
            not self._failure_observed
            and exc_type is None
            and (self._exhausted or task.done())
        ):
            # A clean exit with the producer exhausted or finished: propagate
            # the producer's own outcome.
            await task
            return

        # Otherwise the producer is ancillary: a stream failure was already
        # delivered through iteration/result, or the workflow body is
        # propagating its own exception, or iteration was abandoned early
        # (a clean exit is not proof of exhaustion — breaking from an
        # async-for loop also exits cleanly, and a still-running unbounded
        # producer must not pin the workflow forever). Stop/consume it
        # without raising its failure a second time or replacing the body's.
        if not task.done():
            _ = task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task

    @override
    def __aiter__(self) -> AsyncIterator[_S]:
        reader = self._reader
        if reader is None:
            msg = "StreamCall must be entered before iteration"
            raise RuntimeError(msg)
        return self._iterate(reader)

    async def _iterate(self, reader: Stream[_S]) -> AsyncIterator[_S]:
        # The reader is a single-use buffer: once it has delivered StreamClosed
        # nothing will wake it again, so a fresh generator over a drained
        # reader must terminate rather than wait forever.
        if self._exhausted:
            return
        try:
            async for item in reader:
                yield item
        except StreamClosed as exc:
            self._exhausted = True
            if exc.reason is None:
                return
            self._failure_observed = True
            raise exc.reason from None
        else:
            self._exhausted = True

    async def result(self) -> int:
        """Return the number of items the generator durably yielded.

        This awaits the underlying generator's completion. After abandoning
        iteration early (``break``), await it only once the ``async with``
        block has exited (which stops a still-running producer); inside the
        block an unbounded producer would never finish.

        Returns:
            The count of items the underlying generator produced.

        Raises:
            RuntimeError: if awaited before the call is entered.
            InvalidRunStateError: if the producer was stopped by an early
                context exit before completing.
            CancelledError: if the awaiting caller itself is cancelled.

        """
        if self._task is None:
            msg = "StreamCall must be entered before awaiting its result"
            raise RuntimeError(msg)
        try:
            return await asyncio.shield(self._task)
        except asyncio.CancelledError:
            if self._task.cancelled():
                # The context exit cancelled a producer that iteration never
                # exhausted; there is no count to return. Surface a clear
                # error instead of leaking the internal cancellation.
                msg = (
                    "streaming effect was stopped before completion; its "
                    "result is unavailable after an early exit"
                )
                raise InvalidRunStateError(msg) from None
            raise
        except Exception:
            self._failure_observed = True
            raise


class WorkflowContext:
    """The single object workflow code interacts with.

    Provides effect calls, deterministic utilities, and typed port access.
    """

    __slots__ = (
        "_codec",
        "_ctx",
        "_inputs",
        "_lock",
        "_outputs",
        "_pending",
        "_requests",
        "_signals",
    )

    def __init__(self, ctx: Context, codec: Codec) -> None:
        self._ctx = ctx
        self._codec = codec
        self._inputs: dict[str, Stream[Any]] = {}
        self._pending: dict[str, deque[Any]] = {}
        self._outputs: dict[str, StreamWriter[Any]] = {}
        self._signals: dict[str, _CoreSignal[Any]] = {}
        self._requests: dict[str, StreamWriter[Any]] = {}
        # Serializes first-time stream creation so two coroutines racing on the
        # same fresh port (e.g. under asyncio.gather) cannot each create a
        # duplicate stream between the cache miss and the cache populate.
        self._lock = asyncio.Lock()

    async def _get_or_create(
        self,
        cache: dict[str, _T],
        key: str,
        factory: Callable[[], Coroutine[Any, Any, _T]],
    ) -> _T:
        """Return the cached per-port object, creating it under the lock.

        Creation is double-checked so concurrent first uses of the same port
        produce exactly one underlying stream.

        Returns:
            The cached or newly created object for ``key``.

        """
        value = cache.get(key)
        if value is None:
            async with self._lock:
                value = cache.get(key)
                if value is None:
                    value = await factory()
                    cache[key] = value
        return value

    # --- effects -----------------------------------------------------------

    async def call(
        self, effect: Effect[_P, _T], /, *args: _P.args, **kwargs: _P.kwargs
    ) -> _T:
        """Invoke an effect and durably record its result.

        Returns:
            The effect's decoded result.

        Raises:
            WorkflowDefinitionError: if ``effect`` is a streaming
                (async-generator) effect; use :meth:`stream` for those.

        """
        if effect.is_generator:
            msg = (
                f"effect {effect.name!r} is a streaming (async-generator) effect; "
                "run it with ctx.stream(...), not ctx.call(...)"
            )
            raise WorkflowDefinitionError(msg)
        metadata = effect.op_metadata
        if effect.is_async or effect.executor == "inline":
            return await self._ctx.run_effect(
                metadata, effect.fn, *args, return_type=effect.return_type, **kwargs
            )
        return await self._ctx.run_effect(
            metadata,
            _thread_runner(effect, args, kwargs),
            return_type=effect.return_type,
        )

    def stream(
        self,
        effect: Effect[_P, AsyncIterator[_S]],
        /,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> StreamCall[_S]:
        """Run a streaming (async-generator) effect as a durable stream.

        Returns:
            A :class:`StreamCall` context manager over the recorded items.

        Raises:
            WorkflowDefinitionError: if ``effect`` is not a streaming
                (async-generator) effect; use :meth:`call` for those.

        """
        if not effect.is_generator:
            msg = (
                f"effect {effect.name!r} is not a streaming (async-generator) "
                "effect; run it with ctx.call(...), not ctx.stream(...)"
            )
            raise WorkflowDefinitionError(msg)
        return StreamCall(self._ctx, effect, args, kwargs)

    # --- deterministic utilities ------------------------------------------

    @property
    def operation_id(self) -> str:
        """Deterministic identifier for the current operation position.

        Returns:
            An opaque identifier, stable across replay of this position.

        """
        return self._ctx.operation_id

    @property
    def idempotency_key(self) -> str:
        """A stable idempotency key for an external system.

        Combines the run's identity with the operation position, so a retried
        execution of the same recorded ``ctx.call`` receives the same key.

        Returns:
            A canonical idempotency key string.

        """
        return f"{self._ctx.seed}:{self._ctx.operation_id}"

    async def now(self) -> datetime:
        """Return a deterministic timezone-aware UTC timestamp.

        Returns:
            The current deterministic UTC time.

        """
        return utc_from_us(await self._ctx.time_us())

    async def monotonic(self) -> float:
        """Return a deterministic clock value, for elapsed-time math.

        Reads the same deterministic clock as :meth:`now` (it is not a separate
        monotonic source); within a run it never goes backwards, which is all
        elapsed-time math needs.

        Returns:
            Seconds on the deterministic clock.

        """
        return await self._ctx.time()

    def random(self) -> random.Random:
        """Return a deterministic, replay-stable random number generator.

        Returns:
            A seeded :class:`random.Random` instance.

        """
        return self._ctx.random()

    async def sleep(self, delay: float) -> None:
        """Sleep until a durable deadline (not ``asyncio.sleep``).

        The deadline is recorded as ``now + delay`` when the sleep starts, so a
        run resumed after a crash mid-sleep waits only for the remainder of the
        original deadline instead of restarting the full delay.
        """
        deadline = await self._ctx.time() + delay

        async def sleep_until() -> None:
            remaining = deadline - time.time()
            if remaining > 0:
                await asyncio.sleep(remaining)

        _ = await self._ctx.run(sleep_until)

    # --- inputs ------------------------------------------------------------

    async def receive(self, port: Input[_T], /) -> _T:
        """Consume the earliest unconsumed value from an input port (FIFO).

        Waiting on a closed, drained port raises :class:`~duron.PortClosedError`,
        since the wait could never complete.

        Returns:
            The next value delivered to the port.

        """
        items = await self.receive_many(port, max_items=1, wait=True)
        return items[0]

    async def receive_many(
        self, port: Input[_T], /, *, max_items: int | None = None, wait: bool = True
    ) -> list[_T]:
        """Consume a contiguous run of buffered values from an input port.

        With ``wait=False`` a closed, drained port yields ``[]`` so drain loops
        terminate normally; with ``wait=True`` it raises
        :class:`~duron.PortClosedError`, since the wait could never complete.

        Returns:
            The consumed values, in FIFO order.

        """
        pending = self._pending_for(port)
        if not pending:
            await self._fill(port, pending, block=wait)
        if max_items is None:
            items = list(pending)
            pending.clear()
        else:
            items = [pending.popleft() for _ in range(min(max_items, len(pending)))]
        return cast("list[_T]", items)

    def _pending_for(self, port: Input[Any]) -> deque[Any]:
        pending = self._pending.get(port.name)
        if pending is None:
            pending = self._pending[port.name] = deque()
        return pending

    async def _fill(self, port: Input[_T], pending: deque[Any], *, block: bool) -> None:
        reader = await self._input_reader(port)
        try:
            batch = await reader.next(block=block)
        except StreamClosed as exc:
            if block:
                # A blocking receive on a closed, drained port can never make
                # progress, so surface it. A non-blocking poll instead reports
                # "nothing available" (empty pending) so drain loops that treat
                # [] as "done" terminate rather than crash.
                msg = f"input port {port.name!r} is closed"
                raise PortClosedError(msg) from exc
            return
        pending.extend(batch)

    async def _input_reader(self, port: Input[_T]) -> Stream[_T]:
        async def create() -> Stream[Any]:
            reader, _writer = await self._ctx.create_stream(
                port.item_type, name=port.stream_name
            )
            return reader

        return cast(
            "Stream[_T]", await self._get_or_create(self._inputs, port.name, create)
        )

    # --- outputs -----------------------------------------------------------

    async def emit(self, port: Output[_T], value: _T, /) -> None:
        """Append a value to a durable output port. Returns after it is durable."""
        writer = await self._get_or_create(
            self._outputs,
            port.name,
            lambda: self._ctx.create_sink(port.item_type, name=port.stream_name),
        )
        await writer.send(value)

    # --- requests ----------------------------------------------------------

    async def request(self, port: Request[_Req, _Res], value: _Req, /) -> _Res:
        """Issue a durable request and await its single reply.

        Returns:
            The response supplied by host code.

        """
        request_id, future = await self._ctx.create_future(
            # Pass the declared response type through as-is. For an unsubscripted
            # Request this is the UnspecifiedType sentinel, which must match the
            # hint the host resolves with (PendingRequest.respond passes
            # port.response_type too); coercing to `object` here would diverge
            # from the host under schema-aware codecs.
            cast("type[_Res]", port.response_type),
            error_types=port.raises,
        )
        writer = await self._get_or_create(
            self._requests,
            port.name,
            lambda: self._ctx.create_sink(list[object], name=port.stream_name),
        )
        envelope = encode_request_envelope(
            self._codec, request_id, value, port.request_type
        )
        await writer.send(envelope)
        return await future

    # --- signals -----------------------------------------------------------

    def interruptible(self, signal: Signal[_T], /) -> AbstractAsyncContextManager[None]:
        """Arm a signal for the duration of the block.

        If the signal fires while an operation inside the block is awaiting, the
        block exits by raising :class:`~duron.Interrupted` carrying the payload.

        Returns:
            An async context manager that arms the signal.

        """
        return self._interruptible(signal)

    @contextlib.asynccontextmanager
    async def _interruptible(self, signal: Signal[_T]) -> AsyncGenerator[None]:
        async def create() -> _CoreSignal[Any]:
            core_signal, _writer = await self._ctx.create_signal(
                signal.item_type, name=signal.stream_name
            )
            return core_signal

        core_signal = await self._get_or_create(self._signals, signal.name, create)
        try:
            async with core_signal:
                yield
        except SignalInterrupt as exc:
            raise Interrupted(exc.value) from None


def _thread_runner(
    effect: Effect[..., _T], args: tuple[object, ...], kwargs: dict[str, object]
) -> Callable[[], Coroutine[Any, Any, _T]]:
    async def runner() -> _T:
        return cast("_T", await asyncio.to_thread(effect.fn, *args, **kwargs))

    return runner
