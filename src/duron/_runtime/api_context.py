"""Private implementation of the workflow authoring protocol."""

# pyright: reportPrivateUsage=false
# ruff: file-ignore[private-member-access]

from __future__ import annotations

import asyncio
import contextlib
import inspect
import math
from collections import deque
from typing import TYPE_CHECKING, Any, cast, overload
from typing_extensions import ParamSpec, TypeVar

from duron._api.context import Selection
from duron._api.effect import Effect
from duron._api.effect_run import EffectRun
from duron._api.ports import (
    Emitter,
    Input,
    Output,
    Receiver,
    _check_port,
    _stream_name_for_port,
    _utc_from_us,
)
from duron._execution.models import HandleReference
from duron._execution.registration import schema_id
from duron._execution.validation import validate_schema
from duron._runtime.channels import Commands, Stream
from duron._runtime.context import InputCandidate
from duron.errors import Interrupted, PortClosedError, WorkflowDefinitionError

if TYPE_CHECKING:
    import random
    from collections.abc import AsyncGenerator, Awaitable
    from datetime import datetime

    from duron._api.ports import _Port
    from duron._execution.models import EffectOperation
    from duron._hints import TypeHint
    from duron._runtime.context import Context
    from duron.codec import Codec

_T = TypeVar("_T")
_P = ParamSpec("_P")


class WorkflowContextImpl:
    """The single object workflow code interacts with.

    Provides effect calls, deterministic utilities, and typed port access.
    """

    __slots__ = (
        "_closed_inputs",
        "_codec",
        "_creating",
        "_ctx",
        "_input_locks",
        "_input_offsets",
        "_interrupts",
        "_lock",
        "_outputs",
        "_pending",
        "_ports",
        "_workflow_name",
    )

    def __init__(
        self,
        ctx: Context,
        codec: Codec,
        *,
        workflow_name: str,
        ports: tuple[_Port, ...],
    ) -> None:
        self._ctx = ctx
        self._codec = codec
        self._workflow_name = workflow_name
        self._ports = frozenset(ports)
        self._input_offsets: dict[str, int] = {}
        self._closed_inputs: set[str] = set()
        self._interrupts: set[str] = set()
        self._input_locks: dict[str, asyncio.Lock] = {}
        self._pending: dict[str, deque[Any]] = {}
        self._outputs: dict[str, str] = {}
        # Serializes first-time stream creation so two coroutines racing on the
        # same fresh port (e.g. under asyncio.gather) cannot each create a
        # duplicate stream between the cache miss and the cache populate.
        self._lock = asyncio.Lock()
        self._creating: dict[str, asyncio.Task[str]] = {}

    @overload
    def port(self, declaration: Input[_T], /) -> Receiver[_T]: ...
    @overload
    def port(self, declaration: Output[_T], /) -> Emitter[_T]: ...
    def port(self, declaration: _Port, /) -> Receiver[Any] | Emitter[Any]:
        self._check_port(declaration)
        if isinstance(declaration, Input):
            return Receiver(self, declaration)
        return Emitter(self, declaration)

    def _check_port(self, port: _Port) -> None:
        _check_port(port, self._ports, self._workflow_name)

    def _check_input(self, port: Input[Any]) -> None:
        self._check_port(port)
        if port.name in self._interrupts:
            msg = f"input port {port.name!r} already has an interruptible consumer"
            raise WorkflowDefinitionError(msg)

    async def _output_stream(self, port: Output[Any]) -> str:
        """Create each output stream once, sharing creation across callers.

        Returns:
            The durable output stream identity.

        Raises:
            asyncio.CancelledError: If the caller is cancelled during creation.

        """
        key = port.name
        value = self._outputs.get(key)
        if value is not None:
            return value
        async with self._lock:
            value = self._outputs.get(key)
            if value is not None:
                return value
            task = self._creating.get(key)
            if task is None:
                task = self._ctx.loop.create_task(
                    self._ctx.create_output(port.item_type, _stream_name_for_port(port))
                )
                self._creating[key] = task
        # Do not hold the workflow-wide lock across storage I/O. This lets
        # independent ports initialize concurrently while the shared task still
        # deduplicates callers racing on one port.
        try:
            created = await asyncio.shield(task)
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
                if task.done() and self._creating.get(key) is task:
                    self._creating.pop(key, None)
            raise
        async with self._lock:
            # A cancelled waiter may have left the task in-flight while another
            # caller completed and published the same value.
            cached = self._outputs.get(key)
            if cached is not None:
                return cached
            self._outputs[key] = created
            if self._creating.get(key) is task:
                self._creating.pop(key, None)
        return created

    # --- effects -----------------------------------------------------------

    @contextlib.asynccontextmanager
    async def _open_stream(
        self, item_type: TypeHint[_T], *, name: str | None = None, prefetch: int = 64
    ) -> AsyncGenerator[Stream[_T], None]:
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
        )
        await self._ctx.open_owned_channel(reference, item_type, name)
        stream = Stream(
            self._ctx.loop,
            reference,
            item_type=item_type,
            prefetch=prefetch,
            codec=None,
        )
        try:
            yield stream
        finally:
            await stream._close_scope()

    @contextlib.asynccontextmanager
    async def _open_commands(
        self, item_type: TypeHint[_T], *, name: str | None = None
    ) -> AsyncGenerator[Commands[_T], None]:
        validate_schema(item_type)
        reference = HandleReference(
            self._ctx.seed,
            self._ctx.operation_id,
            "signal.receiver",
            self._ctx.operation_id,
            schema_id(item_type),
        )
        await self._ctx.open_owned_channel(reference, item_type, name)
        commands = Commands(
            self._ctx.loop, reference, item_type=item_type, codec=self._codec
        )
        try:
            yield commands
        finally:
            await commands._close_scope()

    async def call(
        self, effect: Effect[_P, _T], /, *args: _P.args, **kwargs: _P.kwargs
    ) -> _T:
        if not isinstance(cast("object", effect), Effect):
            msg = "ctx.call requires an @duron.effect callable"
            raise WorkflowDefinitionError(msg)
        if effect.ports:
            async with self.start(effect, *args, **kwargs) as run:
                return await run
        operation = self._prepare(effect, args, kwargs)
        return await self._ctx.run_effect(operation, effect.return_type)

    def start(
        self, effect: Effect[_P, _T], /, *args: _P.args, **kwargs: _P.kwargs
    ) -> EffectRun[_T]:
        if not isinstance(cast("object", effect), Effect):
            msg = "ctx.start requires an @duron.effect callable"
            raise WorkflowDefinitionError(msg)
        return EffectRun(self, effect, args, kwargs)

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
            seed=self._ctx.seed,
            owner_id=self._ctx.operation_id,
        )

    # --- deterministic utilities ------------------------------------------

    async def now(self) -> datetime:
        return _utc_from_us(await self._ctx.time_us())

    def random(self) -> random.Random:
        return self._ctx.random()

    async def sleep(self, delay: float) -> None:
        if isinstance(delay, bool) or not math.isfinite(delay) or delay < 0:
            msg = "delay must be finite and nonnegative"
            raise ValueError(msg)
        deadline = await self._ctx.time() + delay

        await self._ctx.sleep_until(deadline)

    async def select(
        self,
        *,
        timeout: float | None = None,  # ruff: ignore[async-function-with-timeout]  # Durable deadline.
        **branches: Receiver[_T] | Awaitable[_T],
    ) -> Selection[_T | None]:
        tasks: dict[str, asyncio.Future[_T]] = {}
        scheduled: set[int] = set()
        try:
            if (
                not branches
                or "timeout" in branches
                or len({
                    id(p._port) if isinstance(p, Receiver) else id(p)
                    for p in branches.values()
                })
                != len(branches)
                or any(
                    not isinstance(value, Receiver) and not inspect.isawaitable(value)
                    for value in branches.values()
                )
            ):
                msg = "select needs distinct inputs or awaitables; timeout is reserved"
                raise ValueError(msg)
            if timeout is not None and (
                isinstance(timeout, bool) or not math.isfinite(timeout) or timeout < 0
            ):
                msg = "timeout must be finite and nonnegative"
                raise ValueError(msg)
            receivers = [
                value for value in branches.values() if isinstance(value, Receiver)
            ]
            if any(receiver._owner is not self for receiver in receivers):
                msg = "select receiver belongs to another workflow context"
                raise WorkflowDefinitionError(msg)
            ports = [receiver._port for receiver in receivers]
            for port in ports:
                self._check_input(port)
                if self._input_lock(port).locked():
                    msg = "input already has an active consumer"
                    raise WorkflowDefinitionError(msg)
            async with contextlib.AsyncExitStack() as stack:
                for port in sorted(ports, key=lambda p: p.name):
                    await stack.enter_async_context(self._input_lock(port))
                    self._interrupts.add(port.name)
                    stack.callback(self._interrupts.remove, port.name)
                deadline = (
                    await self._ctx.time() + timeout if timeout is not None else None
                )
                for name, value in branches.items():
                    if not isinstance(value, Receiver):
                        tasks[name] = asyncio.ensure_future(value)
                        scheduled.add(id(value))
                candidates = [
                    InputCandidate(
                        name,
                        value._port.name,
                        self._input_offsets.get(value._port.name, -1),
                        bool(self._pending_for(value._port)),
                        value._port.name in self._closed_inputs,
                    )
                    if isinstance(value, Receiver)
                    else InputCandidate(name)
                    for name, value in branches.items()
                ]
                choice = await self._ctx.select_input(
                    candidates,
                    deadline,
                    lambda: {name for name, task in tasks.items() if task.done()},
                )
                name = choice["name"]
                if name == "timeout":
                    return Selection("timeout", None)
                if name in tasks:
                    return Selection(name, await tasks[name])
                port = cast("Receiver[_T]", branches[name])._port
                self._input_offsets[port.name] = choice["offset"]
                if choice["closed"]:
                    self._closed_inputs.add(port.name)
                pending = self._pending_for(port)
                pending.extend(
                    self._codec.decode_json(value, port.item_type)
                    for value in choice["values"]
                )
                if not pending:
                    msg = f"input port {port.name!r} is closed"
                    raise PortClosedError(msg)
                return Selection(name, pending.popleft())
        finally:
            # Invalid selections and cancellation before scheduling still own
            # the coroutine objects supplied by their caller.
            for value in branches.values():
                if id(value) not in scheduled and inspect.iscoroutine(value):
                    value.close()
            for task in tasks.values():
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks.values(), return_exceptions=True)
        msg = "selection exited without a result"
        raise AssertionError(msg)

    # --- inputs ------------------------------------------------------------

    async def _receive(self, port: Input[_T], /) -> _T:
        self._check_input(port)
        pending = self._pending_for(port)
        async with self._input_lock(port):
            if not pending:
                await self._fill(port, pending, block=True)
            # A blocking fill either appends at least one value or raises for a
            # closed stream, so this avoids the temporary list used by
            # receive_many(max_items=1).
            return pending.popleft()

    async def _receive_many(
        self, port: Input[_T], /, *, max_items: int | None = None, wait: bool = True
    ) -> list[_T]:
        if max_items is not None and (
            isinstance(max_items, bool)
            or not isinstance(cast("object", max_items), int)
            or max_items <= 0
        ):
            msg = "max_items must be a positive integer or None"
            raise ValueError(msg)
        self._check_input(port)
        pending = self._pending_for(port)
        async with self._input_lock(port):
            if not pending:
                await self._fill(port, pending, block=wait)
            if max_items is None:
                items = list(pending)
                pending.clear()
            else:
                items = [pending.popleft() for _ in range(min(max_items, len(pending)))]
            return items

    def _pending_for(self, port: Input[_T]) -> deque[_T]:
        pending = self._pending.get(port.name)
        if pending is None:
            pending = self._pending[port.name] = deque()
        return cast("deque[_T]", pending)

    def _input_lock(self, port: Input[Any]) -> asyncio.Lock:
        lock = self._input_locks.get(port.name)
        if lock is None:
            lock = self._input_locks[port.name] = asyncio.Lock()
        return lock

    async def _fill(self, port: Input[_T], pending: deque[_T], *, block: bool) -> None:
        if port.name in self._closed_inputs:
            msg = f"input port {port.name!r} is closed"
            raise PortClosedError(msg)
        batch = await self._ctx.read_input(
            port.name, self._input_offsets.get(port.name, -1), block=block
        )
        self._input_offsets[port.name] = batch["offset"]
        if batch["closed"]:
            self._closed_inputs.add(port.name)
        pending.extend(
            self._codec.decode_json(value, port.item_type) for value in batch["values"]
        )
        if not pending and port.name in self._closed_inputs:
            msg = f"input port {port.name!r} is closed"
            raise PortClosedError(msg)

    # --- outputs -----------------------------------------------------------

    async def _emit(
        self, port: Output[_T], value: _T, /, *, idempotency_key: str | None = None
    ) -> None:
        if idempotency_key is not None:
            msg = "workflow emissions use replay-stable operation identities"
            raise ValueError(msg)
        self._check_port(port)
        stream_id = await self._output_stream(port)
        await self._ctx.emit_output(stream_id, value)

    # --- interruption ------------------------------------------------------

    @contextlib.asynccontextmanager
    async def _interruptible(self, port: Input[_T]) -> AsyncGenerator[None]:
        self._check_input(port)
        lock = self._input_lock(port)
        if lock.locked():
            msg = f"input port {port.name!r} already has an active consumer"
            raise WorkflowDefinitionError(msg)
        self._interrupts.add(port.name)
        try:
            async with lock:
                pending = self._pending_for(port)
                if not pending:
                    await self._fill(port, pending, block=False)
                if pending:
                    raise Interrupted(pending.popleft())
                task = asyncio.current_task()
                assert task is not None
                initial_cancellations = task.cancelling()
                interruption: Exception | None = None
                cancelled = False

                async def watch() -> None:
                    nonlocal interruption, cancelled
                    try:
                        await self._fill(port, pending, block=True)
                        interruption = Interrupted(pending.popleft())
                    except Exception as error:  # ruff: ignore[blind-except]
                        interruption = error
                    cancelled = task.cancel()

                watcher = asyncio.create_task(watch())
                try:
                    yield
                finally:
                    _ = watcher.cancel()
                    _ = await asyncio.gather(watcher, return_exceptions=True)
                    self._ctx.loop.generate_op_scope()
                    if cancelled:
                        # Remove the watcher's cancellation; preserve external ones.
                        cancellations = task.cancelling()
                        _ = task.uncancel()
                        if cancellations <= initial_cancellations + 1:
                            assert interruption is not None
                            raise interruption from None
        finally:
            self._interrupts.remove(port.name)
