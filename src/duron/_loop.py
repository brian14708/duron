from __future__ import annotations

import asyncio
import contextlib
import contextvars
import heapq
import logging
import os
from asyncio import (
    AbstractEventLoop,
    CancelledError,
    Handle,
    Task,
    TimerHandle,
    events,
)
from collections import deque
from dataclasses import dataclass
from hashlib import blake2b
from typing import TYPE_CHECKING, Generic, cast, overload
from typing_extensions import (
    TypeVar,
    override,
)

_T = TypeVar("_T")

if TYPE_CHECKING:
    import sys
    from asyncio.futures import Future
    from collections.abc import Callable, Coroutine, Generator
    from contextvars import Context
    from typing_extensions import (
        Any,
        TypeVarTuple,
        Unpack,
    )

    _Ts = TypeVarTuple("_Ts")

    if sys.version_info >= (3, 12):
        _TaskCompatibleCoro = Coroutine[Any, Any, _T]
    else:
        _TaskCompatibleCoro = Generator[Any, None, _T] | Coroutine[Any, Any, _T]


logger = logging.getLogger(__name__)


_task_ctx: contextvars.ContextVar[_TaskCtx] = contextvars.ContextVar("duron_task")


class OpFuture(asyncio.Future[_T], Generic[_T]):
    __slots__: tuple[str, ...] = ("id", "params")

    id: bytes
    params: object

    def __init__(self, id_: bytes, params: object, loop: AbstractEventLoop) -> None:
        super().__init__(loop=loop)
        self.id = id_
        self.params = params


@dataclass(slots=True)
class WaitSet:
    ops: list[OpFuture[object]]
    timer: float | None
    event: asyncio.Event

    async def block(self, now_us: int) -> None:
        if self.timer is None:
            _ = await self.event.wait()
            return
        t = (self.timer - now_us) / 1e6
        if t > 0:
            with contextlib.suppress(asyncio.TimeoutError):
                _ = await asyncio.wait_for(self.event.wait(), timeout=t)


@dataclass(slots=True)
class _TaskCtx:
    parent_id: bytes
    seq: int = 0


class EventLoop(AbstractEventLoop):
    __slots__: tuple[str, ...] = (
        "_closed",
        "_ctx",
        "_debug",
        "_event",
        "_exc_handler",
        "_host",
        "_now_us",
        "_ops",
        "_ready",
        "_timers",
    )

    def __init__(self, host: asyncio.AbstractEventLoop, seed: bytes) -> None:
        self._ready: deque[Handle] = deque()
        self._debug: bool = False
        self._host: asyncio.AbstractEventLoop = host
        self._exc_handler: (
            Callable[[AbstractEventLoop, dict[str, object]], object] | None
        ) = None
        self._ops: dict[bytes, OpFuture[object]] = {}
        self._ctx: _TaskCtx = _TaskCtx(parent_id=seed)
        self._now_us: int = 0
        self._closed: bool = False
        self._event: asyncio.Event = asyncio.Event()  # loop = _host
        self._timers: list[TimerHandle] = []

    def generate_op_id(self) -> bytes:
        ctx = _task_ctx.get(self._ctx)
        ctx.seq += 1
        return _mix_id(ctx.parent_id, ctx.seq - 1)

    def host_loop(self) -> asyncio.AbstractEventLoop:
        return self._host

    @override
    def call_soon(
        self,
        callback: Callable[[Unpack[_Ts]], object],
        *args: Unpack[_Ts],
        context: Context | None = None,
    ) -> Handle:
        h = Handle(
            callback,
            args,
            self,
            context=context,
        )
        self._ready.append(h)
        if asyncio.get_running_loop() is self._host:
            self._event.set()
        return h

    @override
    def call_at(
        self,
        when: float,
        callback: Callable[[Unpack[_Ts]], object],
        *args: Unpack[_Ts],
        context: Context | None = None,
    ) -> TimerHandle:
        th = TimerHandle(
            int(when * 1e6),
            callback,
            args,
            loop=self,
            context=context,
        )
        heapq.heappush(self._timers, th)
        if asyncio.get_running_loop() is self._host:
            self._event.set()
        return th

    @override
    def call_later(
        self,
        delay: float,
        callback: Callable[[Unpack[_Ts]], object],
        *args: Unpack[_Ts],
        context: Context | None = None,
    ) -> TimerHandle:
        th = TimerHandle(
            self.time_us() + int(delay * 1e6),
            callback,
            args,
            loop=self,
            context=context,
        )
        heapq.heappush(self._timers, th)
        if asyncio.get_running_loop() is self._host:
            self._event.set()
        return th

    @override
    def time(self) -> float:
        return self._now_us / 1e6

    def time_us(self) -> int:
        return self._now_us

    def tick(self, time: int) -> None:
        self._now_us = time

    @override
    def create_future(self) -> asyncio.Future[object]:
        return asyncio.Future(loop=self)

    @override
    def create_task(
        self,
        coro: _TaskCompatibleCoro[_T],
        *,
        context: Context | None = None,
        **kwargs: Any,
    ) -> Task[_T]:
        ctx = _context_new_task(context, self.generate_op_id())
        return ctx.run(
            cast("type[Task[_T]]", Task),
            coro,
            loop=self,
            **kwargs,
        )

    def poll_completion(self, task: Future[_T]) -> WaitSet | None:
        old = events.get_running_loop()
        old_task = asyncio.tasks.current_task(old)
        if old_task:
            asyncio.tasks._leave_task(old, old_task)  # noqa: SLF001
        events._set_running_loop(self)  # noqa: SLF001
        try:
            self._event.clear()
            now = self.time_us()
            deadline: float | None = None
            while True:
                deadline = None
                while self._timers:
                    ht = self._timers[0]
                    if ht.cancelled():
                        _ = heapq.heappop(self._timers)
                    elif ht.when() <= now:
                        _ = heapq.heappop(self._timers)
                        self._ready.append(ht)
                    else:
                        deadline = ht.when()
                        break

                if not self._ready:
                    break
                while self._ready:
                    h = self._ready.popleft()
                    if h.cancelled():
                        continue
                    try:
                        h._run()  # noqa: SLF001
                    except Exception as exc:  # noqa: BLE001
                        self.call_exception_handler({
                            "message": "exception in callback",
                            "exception": exc,
                            "handle": h,
                        })

            if task.done():
                return None
            return WaitSet(
                ops=list(self._ops.values()),
                timer=deadline,
                event=self._event,
            )
        finally:
            events._set_running_loop(old)  # noqa: SLF001
            if old_task:
                asyncio.tasks._enter_task(old, old_task)  # noqa: SLF001

    def create_op(self, params: object, *, external: bool = False) -> OpFuture[object]:
        if external:
            id_ = os.urandom(12)
            self._event.set()
        else:
            id_ = self.generate_op_id()
        op_fut: OpFuture[object] = OpFuture(id_, params, self)
        self._ops[id_] = op_fut
        return op_fut

    @overload
    def post_completion(
        self,
        id_: bytes,
        *,
        result: object,
    ) -> None: ...
    @overload
    def post_completion(
        self,
        id_: bytes,
        *,
        exception: BaseException,
    ) -> None: ...
    def post_completion(
        self,
        id_: bytes,
        *,
        result: object = None,
        exception: BaseException | None = None,
    ) -> None:
        if op := self._ops.pop(id_, None):
            if op.done():
                return
            tid = _mix_id(op.id, -1)
            if exception is None:
                _ = self.call_soon(
                    op.set_result,
                    result,
                    context=_context_new_task(None, tid),
                )
            elif type(exception) is CancelledError:
                _ = self.call_soon(
                    op.cancel,
                    context=_context_new_task(None, tid),
                )
            else:
                _ = self.call_soon(
                    op.set_exception,
                    exception,
                    context=_context_new_task(None, tid),
                )

    @override
    def is_closed(self) -> bool:
        return self._closed

    @override
    def close(self) -> None:
        while self._timers:
            th = heapq.heappop(self._timers)
            th.cancel()
        while self._ready:
            h = self._ready.popleft()
            h.cancel()
        self._closed = True

    @override
    def get_debug(self) -> bool:
        return self._debug

    @override
    def set_debug(self, enabled: bool) -> None:
        self._debug = enabled

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
        handler: Callable[[AbstractEventLoop, dict[str, object]], object] | None,
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
    async def shutdown_default_executor(self) -> None:
        pass

    def _timer_handle_cancelled(self, _th: TimerHandle) -> None:
        pass


def _context_new_task(context: Context | None, task_id: bytes) -> Context:
    base = context.copy() if context is not None else contextvars.copy_context()
    _ = base.run(_task_ctx.set, _TaskCtx(parent_id=task_id))
    return base


def _mix_id(a: bytes, b: int) -> bytes:
    return blake2b(b.to_bytes(4, "little", signed=True) + a, digest_size=12).digest()


def create_loop(
    parent_loop: asyncio.AbstractEventLoop,
    seed: bytes,
) -> EventLoop:
    return EventLoop(parent_loop, seed)  # type: ignore[abstract]


def _copy_future_state(source: asyncio.Future[_T], dest: asyncio.Future[_T]) -> None:
    assert source.done()  # noqa: S101
    if dest.cancelled():
        return
    assert not dest.done()  # noqa: S101
    if source.cancelled():
        _ = dest.cancel()
    else:
        exception = source.exception()
        if exception is not None:
            dest.set_exception(exception)
        else:
            result = source.result()
            dest.set_result(result)


def wrap_future(
    future: asyncio.Future[_T],
    *,
    loop: asyncio.AbstractEventLoop | None = None,
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
