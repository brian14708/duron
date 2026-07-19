from __future__ import annotations

import asyncio
import binascii
import contextlib
import contextvars
import logging
import math
from asyncio import events, tasks
from collections import deque
from dataclasses import dataclass
from hashlib import blake2b
from heapq import heappop, heappush
from typing import TYPE_CHECKING, Any, cast, overload
from typing_extensions import TypeVar, TypeVarTuple, Unpack, override

from duron._runtime.storage.helper import random_id
from duron.errors import RemoteEffectError

if TYPE_CHECKING:
    from asyncio import CancelledError
    from asyncio.futures import Future
    from collections.abc import Callable, Coroutine, Generator, Sequence
    from contextvars import Context

    _T = TypeVar("_T")
    _Ts = TypeVarTuple("_Ts")


logger = logging.getLogger("duron._runtime.loop")
_task_ctx: contextvars.ContextVar[_TaskCtx] = contextvars.ContextVar("duron.task")


class OpFuture(asyncio.Future[object]):
    __slots__: tuple[str, ...] = ("external", "id", "params")

    def __init__(
        self,
        id_: str,
        params: object,
        loop: asyncio.AbstractEventLoop,
        *,
        external: bool,
    ) -> None:
        super().__init__(loop=loop)
        self.id = id_
        self.params = params
        self.external = external


@dataclass(slots=True)
class WaitSet:
    added: Sequence[OpFuture]
    timer: int | None
    event: asyncio.Event

    async def block(self, now_us: int, max_timeout_us: int = -1) -> None:
        if self.timer is None and max_timeout_us < 0:
            _ = await self.event.wait()
            return

        if self.timer is None or (
            max_timeout_us > 0 and (self.timer - now_us) > max_timeout_us
        ):
            t = max_timeout_us * 1e-6
        else:
            t = (self.timer - now_us) * 1e-6
        if t > 0:
            with contextlib.suppress(asyncio.TimeoutError):
                _ = await asyncio.wait_for(self.event.wait(), timeout=t)


@dataclass(slots=True)
class _TaskCtx:
    parent_id: str
    seq: int = 0


class LoopClosedError(RuntimeError): ...


class EventLoop(asyncio.AbstractEventLoop):
    __slots__: tuple[str, ...] = (
        "_added",
        "_closed",
        "_event",
        "_exc_handler",
        "_host",
        "_now_us",
        "_ops",
        "_ready",
        "_root_task_seq",
        "_timers",
    )

    def __init__(self, host: asyncio.AbstractEventLoop) -> None:
        self._ready: deque[asyncio.Handle] = deque()
        self._host: asyncio.AbstractEventLoop = host
        self._exc_handler: (
            Callable[[asyncio.AbstractEventLoop, dict[str, object]], object] | None
        ) = None
        self._ops: dict[str, OpFuture] = {}
        self._root_task_seq: int = 0
        self._now_us: int = 0
        self._closed: bool = False
        self._event: asyncio.Event = asyncio.Event()  # loop = _host
        self._timers: list[asyncio.TimerHandle] = []
        self._added: list[OpFuture] = []

    @staticmethod
    def generate_op_id() -> str:
        ctx = _task_ctx.get()
        ctx.seq += 1
        return _derive_id(ctx.parent_id, context=(ctx.seq - 1).to_bytes(4, "big"))

    @staticmethod
    def peek_op_id() -> str | None:
        """Return the id ``generate_op_id`` would produce next, without consuming it.

        Returns:
            The id for the current operation position, or None when called
            outside a durable task context.

        """
        try:
            ctx = _task_ctx.get()
        except LookupError:
            return None
        return _derive_id(ctx.parent_id, context=ctx.seq.to_bytes(4, "big"))

    @staticmethod
    def generate_op_scope() -> None:
        ctx = _task_ctx.get()
        ctx.parent_id = _derive_id(ctx.parent_id)
        ctx.seq = 0

    @override
    def call_soon(
        self,
        callback: Callable[[Unpack[_Ts]], object],
        *args: *_Ts,
        context: Context | None = None,
    ) -> asyncio.Handle:
        h = asyncio.Handle(callback, list(args), self, context=context)
        self._ready.append(h)
        if asyncio.get_running_loop() is self._host:
            self._event.set()
        return h

    @override
    def call_at(
        self,
        when: float,
        callback: Callable[[Unpack[_Ts]], object],
        *args: *_Ts,
        context: Context | None = None,
    ) -> asyncio.TimerHandle:
        t = math.ceil(when * 1e6 - self._now_us) + self._now_us
        th = asyncio.TimerHandle(t, callback, list(args), loop=self, context=context)
        heappush(self._timers, th)
        if asyncio.get_running_loop() is self._host:
            self._event.set()
        return th

    @override
    def call_later(
        self,
        delay: float,
        callback: Callable[[Unpack[_Ts]], object],
        *args: *_Ts,
        context: Context | None = None,
    ) -> asyncio.TimerHandle:
        th = asyncio.TimerHandle(
            self._now_us + math.ceil(delay * 1e6),
            callback,
            list(args),
            loop=self,
            context=context,
        )
        heappush(self._timers, th)
        if asyncio.get_running_loop() is self._host:
            self._event.set()
        return th

    @override
    def time(self) -> float:
        return self._now_us * 1e-6

    def tick(self, time: int) -> None:
        self._now_us = time

    @override
    def create_future(self) -> asyncio.Future[object]:
        return asyncio.Future(loop=self)

    @override
    def create_task(
        self, coro: Generator[Any, None, _T] | Coroutine[Any, Any, _T], **kwargs: Any
    ) -> asyncio.Task[_T]:
        if asyncio.get_running_loop() is not self:
            msg = "EventLoop tasks must be created on the event loop"
            raise RuntimeError(msg)
        task_ctx = _TaskCtx(parent_id=self.generate_op_id())
        context = kwargs.get("context")
        if context is not None:
            # Explicit asyncio contexts still need a distinct durable scope;
            # copying also prevents siblings from sharing the mutable counter.
            context = context.copy()
            context.run(_task_ctx.set, task_ctx)
            kwargs["context"] = context
        token = _task_ctx.set(task_ctx)
        try:
            return asyncio.Task(coro, **kwargs, loop=self)
        finally:
            _task_ctx.reset(token)

    def create_isolated_task(self, coro: Coroutine[Any, Any, _T]) -> asyncio.Task[_T]:
        """Reserve a child scope now and run with its own counter.

        Returns:
            A task isolated from the caller's operation counter.

        Raises:
            RuntimeError: If called outside this workflow loop.

        """
        if asyncio.get_running_loop() is not self:
            msg = "EventLoop tasks must be created on the event loop"
            raise RuntimeError(msg)
        context = contextvars.copy_context()
        context.run(_task_ctx.set, _TaskCtx(parent_id=self.generate_op_id()))
        return context.run(asyncio.Task, coro, loop=self)

    def schedule_task(
        self,
        coro: Generator[Any, None, _T] | Coroutine[Any, Any, _T],
        name: str | None = None,
    ) -> asyncio.Task[_T]:
        if asyncio.get_running_loop() is not self._host:
            msg = "EventLoop scheduling must be called from the host loop"
            raise RuntimeError(msg)
        self._root_task_seq += 1
        id_ = _derive_id("", context=(self._root_task_seq - 1).to_bytes(4, "big"))

        token = _task_ctx.set(_TaskCtx(parent_id=id_))
        task = asyncio.Task(coro, loop=self, name=name)
        _task_ctx.reset(token)
        return task

    def _poll(self, now: int) -> int | None:
        timers = self._timers
        ready = self._ready
        ready_append = ready.append
        ready_popleft = ready.popleft
        while True:
            deadline: int | None = None
            while timers:
                ht = timers[0]
                if ht._cancelled:  # ruff: ignore[private-member-access]
                    _ = heappop(timers)
                elif (t := int(ht.when())) <= now:
                    _ = heappop(timers)
                    ready_append(ht)
                else:
                    deadline = t
                    break

            if not ready:
                return deadline

            while ready:
                h = ready_popleft()
                if h._cancelled:  # ruff: ignore[private-member-access]
                    continue
                h._run()  # ruff: ignore[private-member-access]

    def advance(self) -> int | None:
        """Run ready continuations without draining newly allocated operations.

        Returns:
            The next timer deadline, if any.

        Raises:
            RuntimeError: If called outside the host loop.

        """
        if asyncio.get_running_loop() is not self._host:
            msg = "EventLoop polling must be called from the host loop"
            raise RuntimeError(msg)
        # hot path - inline task context switch
        if prev_task := tasks.current_task():
            tasks._leave_task(self._host, prev_task)  # ruff: ignore[private-member-access]
        events._set_running_loop(self)  # ruff: ignore[private-member-access]
        try:
            return self._poll(self._now_us)
        finally:
            events._set_running_loop(self._host)  # ruff: ignore[private-member-access]
            if prev_task:
                tasks._enter_task(self._host, prev_task)  # ruff: ignore[private-member-access]

    def poll_completion(self, task: Future[_T]) -> WaitSet | None:
        self._event.clear()
        next_deadline = self.advance()
        added = self._added
        self._added = []
        if task.done() and not added:
            return None
        return WaitSet(added=added, timer=next_deadline, event=self._event)

    def pending_ops(self) -> Sequence[OpFuture]:
        return tuple(self._ops.values())

    def create_op(self, params: object) -> OpFuture:
        if self._closed:
            raise LoopClosedError
        if asyncio.get_running_loop() is self._host:
            id_ = random_id()
            external = True
            self._event.set()
        else:
            id_ = self.generate_op_id()
            external = False
        op_fut = OpFuture(id_, params, self, external=external)
        self._ops[id_] = op_fut
        self._added.append(op_fut)
        op_fut.add_done_callback(lambda f: self._ops.pop(f.id, None))
        return op_fut

    @overload
    def post_completion(self, id_: str, *, result: object) -> bool: ...
    @overload
    def post_completion(
        self, id_: str, *, exception: Exception | asyncio.CancelledError
    ) -> bool: ...
    def post_completion(
        self,
        id_: str,
        *,
        result: object = None,
        exception: Exception | asyncio.CancelledError | None = None,
    ) -> bool:
        if op := self._ops.pop(id_, None):
            if op.done() and not op.cancelled():
                msg = "operation already completed"
                raise RuntimeError(msg)
            tid = _derive_id(op.id)
            token = _task_ctx.set(_TaskCtx(parent_id=tid))
            _ = self.call_soon(_complete_op, op, result, exception)
            _task_ctx.reset(token)
            return True
        return False

    @override
    def is_closed(self) -> bool:
        return self._closed

    @override
    def close(self) -> None:
        if asyncio.get_running_loop() is not self._host:
            msg = "EventLoop closing must be called from the host loop"
            raise RuntimeError(msg)
        if prev_task := tasks.current_task():
            tasks._leave_task(self._host, prev_task)  # ruff: ignore[private-member-access]
        events._set_running_loop(self)  # ruff: ignore[private-member-access]
        try:
            # Cleanup may run workflow finally blocks. Reject new durable ops
            # while still allowing the cancellation callbacks to drain.
            self._closed = True
            _ = self._poll(self._now_us)
            to_cancel = (*tasks.all_tasks(), *self._ops.values())
            for t in to_cancel:
                t.cancel()
            _ = asyncio.gather(*to_cancel, return_exceptions=True)
            _ = self._poll(self._now_us)
            for task in to_cancel:
                if task.cancelled():
                    continue
                if not task.done():
                    self.call_exception_handler({
                        "message": "unable to properly cancel task",
                        "task": task,
                    })
                elif task.exception() is not None:
                    self.call_exception_handler({
                        "message": "unhandled exception during asyncio.run() shutdown",
                        "exception": task.exception(),
                        "task": task,
                    })
            self._closed = True
        finally:
            events._set_running_loop(self._host)  # ruff: ignore[private-member-access]
            if prev_task:
                tasks._enter_task(self._host, prev_task)  # ruff: ignore[private-member-access]

    @override
    def get_debug(self) -> bool:
        return False

    @override
    def default_exception_handler(self, context: dict[str, object]) -> None:
        msg = context.get("message", "Unhandled exception")
        exc = context.get("exception")
        if exc:
            logger.error("%s: %r", msg, exc)
        else:
            logger.error("%s", msg)

    @override
    def set_exception_handler(
        self,
        handler: Callable[[asyncio.AbstractEventLoop, dict[str, object]], object]
        | None,
    ) -> None:
        self._exc_handler = handler

    @override
    def call_exception_handler(self, context: dict[str, object]) -> None:
        if self._exc_handler is None:
            self.default_exception_handler(context)
        else:
            _ = self._exc_handler(self, context)

    @override
    async def shutdown_asyncgens(self) -> None:
        pass

    @override
    async def shutdown_default_executor(self, timeout: float | None = None) -> None:
        pass

    def _timer_handle_cancelled(self, _th: asyncio.TimerHandle) -> None:
        pass


async def create_loop() -> EventLoop:  # ruff: ignore[unused-async]
    # asyncio's stub declares unused loop methods abstract; runtime permits this.
    loop_factory = cast("Callable[[asyncio.AbstractEventLoop], EventLoop]", EventLoop)
    return loop_factory(asyncio.get_running_loop())


def _copy_future_state(source: asyncio.Future[_T], dest: asyncio.Future[_T]) -> None:
    assert source.done()
    if dest.cancelled():
        return
    assert not dest.done()
    if source.cancelled():
        _ = dest.cancel()
    elif (exception := source.exception()) is not None:
        dest.set_exception(exception)
    else:
        dest.set_result(source.result())


def _complete_op(
    op: OpFuture, result: object, exception: Exception | CancelledError | None
) -> None:
    if op.cancelled():
        return
    if exception is not None:
        if isinstance(exception, StopIteration):
            # asyncio.Future.set_exception rejects StopIteration; delivering it
            # raw would raise inside this callback and leave the op forever
            # pending (the awaiting workflow would hang). Wrap it instead.
            wrapped = RemoteEffectError(
                type(exception).__qualname__, str(exception), ()
            )
            wrapped.__cause__ = exception
            exception = wrapped
        op.set_exception(exception)
    else:
        op.set_result(result)


def wrap_future(
    future: asyncio.Future[_T], *, loop: asyncio.AbstractEventLoop | None = None
) -> asyncio.Future[_T]:
    src_loop = future.get_loop()
    dst_loop = loop or asyncio.get_running_loop()
    if src_loop is dst_loop:
        return future
    dst_future: asyncio.Future[_T] = dst_loop.create_future()

    def done(f: asyncio.Future[_T]) -> None:
        _ = dst_loop.call_soon(_copy_future_state, f, dst_future)

    def dst_done(f: asyncio.Future[_T]) -> None:
        if f.cancelled() and not future.done():
            _ = src_loop.call_soon(future.cancel)

    future.add_done_callback(done)
    dst_future.add_done_callback(dst_done)
    return dst_future


def _derive_id(base: str, *, context: bytes = b"", key: bytes = b"") -> str:
    return binascii.b2a_base64(
        blake2b(
            binascii.a2b_base64(base), salt=context, key=key, digest_size=12
        ).digest(),
        newline=False,
    ).decode()
