"""Private implementation of the workflow authoring protocol."""

from __future__ import annotations

import asyncio
import contextlib
import time
import warnings
from collections import deque
from typing import TYPE_CHECKING, Any, cast
from typing_extensions import ParamSpec, TypeVar

from duron._api.ports import encode_request_envelope, utc_from_us
from duron._runtime.execution.effect import Effect
from duron._runtime.execution.executor import schema_id
from duron._runtime.execution.handle_data import HandleReference
from duron._runtime.execution.handles import CommandReceiver, StreamSender
from duron._runtime.execution.value_schema import validate_schema
from duron._runtime.workflow.owned_signal import DurableCommands
from duron._runtime.workflow.owned_stream import DurableStream
from duron._runtime.workflow.signal import SignalInterrupt
from duron._runtime.workflow.stream import StreamClosed
from duron.errors import Interrupted, PortClosedError, WorkflowDefinitionError

if TYPE_CHECKING:
    import random
    from collections.abc import AsyncGenerator, Callable, Coroutine
    from contextlib import AbstractAsyncContextManager
    from datetime import datetime

    from duron._api.dependencies import Dependency
    from duron._api.ports import Input, Interrupt, Output, Request
    from duron._runtime.execution.models import EffectOperation
    from duron._runtime.workflow.context import Context
    from duron._runtime.workflow.signal import Signal as _CoreSignal
    from duron._runtime.workflow.stream import Stream, StreamWriter
    from duron.codec import Codec
    from duron.types import TypeHint

_T = TypeVar("_T")
_Req = TypeVar("_Req")
_Res = TypeVar("_Res")
_P = ParamSpec("_P")


class WorkflowContextImpl:
    """The single object workflow code interacts with.

    Provides effect calls, deterministic utilities, and typed port access.
    """

    __slots__ = (
        "_codec",
        "_codec_id",
        "_creating",
        "_ctx",
        "_input_locks",
        "_inputs",
        "_lock",
        "_outputs",
        "_pending",
        "_requests",
        "_requires",
        "_signals",
        "_warned",
        "_workflow_name",
    )

    def __init__(
        self,
        ctx: Context,
        codec: Codec,
        *,
        codec_id: str,
        workflow_name: str,
        requires: tuple[Dependency, ...],
    ) -> None:
        self._ctx = ctx
        self._codec = codec
        self._codec_id = codec_id
        self._workflow_name = workflow_name
        self._requires = frozenset(requires)
        self._warned: set[Dependency] = set()
        self._inputs: dict[str, Stream[Any]] = {}
        self._input_locks: dict[str, asyncio.Lock] = {}
        self._pending: dict[str, deque[Any]] = {}
        self._outputs: dict[str, StreamWriter[Any]] = {}
        self._signals: dict[str, _CoreSignal[Any]] = {}
        self._requests: dict[str, StreamWriter[Any]] = {}
        # Serializes first-time stream creation so two coroutines racing on the
        # same fresh port (e.g. under asyncio.gather) cannot each create a
        # duplicate stream between the cache miss and the cache populate.
        self._lock = asyncio.Lock()
        self._creating: dict[tuple[int, str], asyncio.Task[Any]] = {}

    def _check_dependency(self, dependency: Dependency) -> None:
        if dependency in self._requires or dependency in self._warned:
            return
        warnings.warn(
            f"workflow {self._workflow_name!r} used undeclared "
            f"{type(dependency).__name__} {dependency.name!r}; "
            "add this object to @duron.workflow(requires=(...))",
            UserWarning,
            stacklevel=3,
        )
        self._warned.add(dependency)

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

        Raises:
            asyncio.CancelledError: if the caller is cancelled while creating
                the underlying durable stream.

        """
        value = cache.get(key)
        if value is not None:
            return value
        creation_key = (id(cache), key)
        async with self._lock:
            value = cache.get(key)
            if value is not None:
                return value
            task = self._creating.get(creation_key)
            if task is None:
                task = self._ctx.loop.create_isolated_task(factory())
                self._creating[creation_key] = task
        # Do not hold the workflow-wide lock across storage I/O. This lets
        # independent ports initialize concurrently while the shared task still
        # deduplicates callers racing on one port.
        try:
            created = await asyncio.shield(cast("asyncio.Task[_T]", task))
        except asyncio.CancelledError:
            # Cancellation is intentionally left in the map: stream creation
            # may have committed before cancellation was observed, so retrying
            # it could create a duplicate durable stream.
            raise
        except Exception:
            # A failed creation must not poison this port forever. Keep an
            # in-flight task available to other callers after cancellation,
            # but discard it once it has completed with an error so a later
            # operation can retry a transient storage failure.
            async with self._lock:
                if task.done() and self._creating.get(creation_key) is task:
                    self._creating.pop(creation_key, None)
            raise
        async with self._lock:
            # A cancelled waiter may have left the task in-flight while another
            # caller completed and published the same value.
            cached = cache.get(key)
            if cached is not None:
                return cached
            cache[key] = created
            if self._creating.get(creation_key) is task:
                self._creating.pop(creation_key, None)
        return created

    # --- effects -----------------------------------------------------------

    @contextlib.asynccontextmanager
    async def open_stream(
        self, item_type: TypeHint[_T], *, name: str | None = None, prefetch: int = 64
    ) -> AsyncGenerator[DurableStream[_T], None]:
        validate_schema(item_type)
        if type(prefetch) is not int or prefetch <= 0:
            msg = "stream prefetch must be a positive integer"
            raise ValueError(msg)
        reference = HandleReference(
            self._ctx.seed,
            self._ctx.operation_id,
            "stream.sender",
            self._ctx.operation_id,
            schema_id(item_type),
            self._codec_id,
        )
        await self._ctx.open_owned_channel(reference, item_type, name)
        stream = DurableStream(
            self._ctx.loop, reference, item_type=item_type, prefetch=prefetch
        )
        try:
            yield stream
        finally:
            await stream.close_scope()

    @contextlib.asynccontextmanager
    async def open_commands(
        self, item_type: TypeHint[_T], *, name: str | None = None
    ) -> AsyncGenerator[DurableCommands[_T], None]:
        validate_schema(item_type)
        reference = HandleReference(
            self._ctx.seed,
            self._ctx.operation_id,
            "signal.receiver",
            self._ctx.operation_id,
            schema_id(item_type),
            self._codec_id,
        )
        await self._ctx.open_owned_channel(reference, item_type, name)
        commands = DurableCommands(
            self._ctx.loop, reference, item_type=item_type, codec=self._codec
        )
        try:
            yield commands
        finally:
            await commands.close_scope()

    async def call(
        self, effect: Effect[_P, _T], /, *args: _P.args, **kwargs: _P.kwargs
    ) -> _T:
        if not isinstance(cast("object", effect), Effect):
            msg = "ctx.call requires an @duron.effect callable"
            raise WorkflowDefinitionError(msg)
        self._check_dependency(effect)
        operation = self._prepare(effect, args, kwargs)
        for value in (*args, *kwargs.values()):
            if isinstance(value, (StreamSender, CommandReceiver)):
                value.track_producer()
        return await self._ctx.run_effect(operation, effect.return_type)

    def _prepare(
        self,
        effect: Effect[..., Any],
        args: tuple[object, ...],
        kwargs: dict[str, object],
    ) -> EffectOperation:
        return self._ctx.executor.prepare(
            effect,
            args,
            kwargs,
            codec=self._codec,
            codec_id=self._codec_id,
            seed=self._ctx.seed,
            owner_id=self._ctx.operation_id,
        )

    # --- deterministic utilities ------------------------------------------

    async def now(self) -> datetime:
        return utc_from_us(await self._ctx.time_us())

    def random(self) -> random.Random:
        return self._ctx.random()

    async def sleep(self, delay: float) -> None:
        deadline = await self._ctx.time() + delay

        async def sleep_until() -> None:
            remaining = deadline - time.time()
            if remaining > 0:
                await asyncio.sleep(remaining)

        _ = await self._ctx.run(sleep_until)

    # --- inputs ------------------------------------------------------------

    async def receive(self, port: Input[_T], /) -> _T:
        self._check_dependency(port)
        pending = self._pending_for(port)
        async with self._input_lock(port):
            if not pending:
                await self._fill(port, pending, block=True)
            # A blocking fill either appends at least one value or raises for a
            # closed stream, so this avoids the temporary list used by
            # receive_many(max_items=1).
            return cast("_T", pending.popleft())

    async def receive_many(
        self, port: Input[_T], /, *, max_items: int | None = None, wait: bool = True
    ) -> list[_T]:
        if max_items is not None and (
            isinstance(max_items, bool)
            or not isinstance(cast("object", max_items), int)
            or max_items <= 0
        ):
            msg = "max_items must be a positive integer or None"
            raise ValueError(msg)
        self._check_dependency(port)
        pending = self._pending_for(port)
        async with self._input_lock(port):
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

    def _input_lock(self, port: Input[Any]) -> asyncio.Lock:
        lock = self._input_locks.get(port.name)
        if lock is None:
            lock = self._input_locks[port.name] = asyncio.Lock()
        return lock

    async def _fill(self, port: Input[_T], pending: deque[Any], *, block: bool) -> None:
        reader = await self._input_reader(port)
        try:
            batch = await reader.next(block=block)
        except StreamClosed as exc:
            msg = f"input port {port.name!r} is closed"
            raise PortClosedError(msg) from exc
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
        self._check_dependency(port)
        writer = await self._get_or_create(
            self._outputs,
            port.name,
            lambda: self._ctx.create_sink(port.item_type, name=port.stream_name),
        )
        await writer.send(value)

    # --- requests ----------------------------------------------------------

    async def request(self, port: Request[_Req, _Res], value: _Req, /) -> _Res:
        self._check_dependency(port)
        request_id, future = await self._ctx.create_future(
            # Use the same declared response type as PendingRequest.respond;
            # coercing to object would diverge under schema-aware codecs.
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

    def interruptible(
        self, signal: Interrupt[_T], /
    ) -> AbstractAsyncContextManager[None]:
        self._check_dependency(signal)
        return self._interruptible(signal)

    @contextlib.asynccontextmanager
    async def _interruptible(self, signal: Interrupt[_T]) -> AsyncGenerator[None]:
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
