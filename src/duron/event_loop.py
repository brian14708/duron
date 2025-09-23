from __future__ import annotations

import asyncio
import contextlib
import contextvars
import heapq
import logging
import threading
from asyncio import AbstractEventLoop, Handle, Task, TimerHandle, events
from collections import deque
from dataclasses import dataclass
from hashlib import blake2b
from typing import TYPE_CHECKING, cast, overload

from typing_extensions import (
    Any,
    TypeVar,
    TypeVarTuple,
    Unpack,
    override,
)

if TYPE_CHECKING:
    import sys
    from asyncio.futures import Future
    from collections.abc import Callable, Coroutine, Generator
    from contextvars import Context

    _T = TypeVar("_T")
    _Ts = TypeVarTuple("_Ts")

    if sys.version_info >= (3, 12):
        _TaskCompatibleCoro = Coroutine[Any, Any, _T]
    else:
        _TaskCompatibleCoro = Generator[Any, None, _T] | Coroutine[Any, Any, _T]


logger = logging.getLogger(__name__)


_task_ctx: contextvars.ContextVar[_TaskCtx] = contextvars.ContextVar("duron_task")


class OpFuture(asyncio.Future[object]):
    id: bytes
    params: object

    def __init__(self, id: bytes, params: object, loop: EventLoop) -> None:
        super().__init__(loop=loop)
        self.id = id
        self.params = params


@dataclass(slots=True)
class WaitSet:
    ops: list[OpFuture]
    timer: float | None
    event: asyncio.Event

    async def wait(self, now_ns: int) -> None:
        if self.timer is None:
            _ = await self.event.wait()
            return
        with contextlib.suppress(asyncio.TimeoutError):
            t = self.timer - (now_ns / 1e9)
            if t > 0:
                _ = await asyncio.wait_for(self.event.wait(), timeout=t)


@dataclass(slots=True)
class _TaskCtx:
    parent_id: bytes
    seq: int = 0


class EventLoop(AbstractEventLoop):
    def __init__(self, seed: bytes) -> None:
        self._ready: deque[Handle] = deque()
        self._debug: bool = False
        self._exc_handler: (
            Callable[[AbstractEventLoop, dict[str, object]], object] | None
        ) = None
        self._ops: dict[bytes, OpFuture] = {}
        self._ctx: _TaskCtx = _TaskCtx(parent_id=seed)
        self._now_ns: int = 0
        self._closed: bool = False
        self._event: asyncio.Event = asyncio.Event()

        self._lock: threading.Lock = threading.Lock()
        self._timers: list[TimerHandle] = []

    def _generate_id(self) -> bytes:
        ctx = _task_ctx.get(self._ctx)
        ctx.seq += 1
        return _mix_id(ctx.parent_id, ctx.seq - 1)

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
        return h

    @override
    def call_soon_threadsafe(
        self,
        callback: Callable[[Unpack[_Ts]], object],
        *args: Unpack[_Ts],
        context: Context | None = None,
        task_id: bytes | None = None,
    ) -> Handle:
        self._event.set()
        h = TimerHandle(
            0,
            callback,
            args,
            self,
            context=self._context_with_task_id(context, task_id=task_id),
        )
        with self._lock:
            heapq.heappush(self._timers, h)
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
            when,
            callback,
            args,
            loop=self,
            context=self._context_with_task_id(context),
        )
        with self._lock:
            heapq.heappush(self._timers, th)
        return th

    @override
    def call_later(
        self,
        delay: float,
        callback: Callable[[Unpack[_Ts]], object],
        *args: Unpack[_Ts],
        context: Context | None = None,
    ) -> TimerHandle:
        return self.call_at(self.time() + delay, callback, *args, context=context)

    @override
    def time(self) -> float:
        return self._now_ns / 1e9

    def tick(self, time: int) -> None:
        self._now_ns = time

    @override
    def create_future(self) -> asyncio.Future[object]:
        return asyncio.Future(loop=self)

    @override
    def create_task(
        self,
        coro: _TaskCompatibleCoro[_T],
        *,
        name: str | None = None,
        context: Context | None = None,
        **kwargs: Any,
    ) -> Task[_T]:
        ctx = self._context_with_task_id(context)
        return ctx.run(
            cast("type[Task[_T]]", Task), coro, name=name, loop=self, **kwargs
        )

    def poll_completion(self, task: Future[_T]) -> WaitSet | None:
        old = events._get_running_loop()
        events._set_running_loop(self)
        try:
            self._event.clear()
            now = self.time()
            deadline: float | None = None
            while True:
                deadline = None
                with self._lock:
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
                        h._run()
                    except BaseException as exc:
                        self.call_exception_handler(
                            {
                                "message": "exception in callback",
                                "exception": exc,
                                "handle": h,
                            }
                        )

            if task.done():
                return None
            return WaitSet(
                ops=list(self._ops.values()),
                timer=deadline,
                event=self._event,
            )
        finally:
            events._set_running_loop(old)

    def create_op(self, params: object) -> OpFuture:
        id = self._generate_id()
        s = OpFuture(id, params, self)
        self._ops[id] = s
        return s

    @overload
    def post_completion_threadsafe(
        self,
        id: bytes,
        *,
        result: object,
    ) -> None: ...
    @overload
    def post_completion_threadsafe(
        self,
        id: bytes,
        *,
        exception: BaseException,
    ) -> None: ...
    def post_completion_threadsafe(
        self,
        id: bytes,
        *,
        result: object = None,
        exception: BaseException | None = None,
    ) -> None:
        if op := self._ops.pop(id, None):
            tid = _mix_id(op.id, -1)
            if exception is not None:
                _ = self.call_soon_threadsafe(op.set_exception, exception, task_id=tid)
            else:
                _ = self.call_soon_threadsafe(op.set_result, result, task_id=tid)

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
        self, handler: Callable[[AbstractEventLoop, dict[str, object]], object] | None
    ) -> None:
        self._exc_handler = handler

    @override
    def call_exception_handler(self, context: dict[str, object]) -> None:
        if self._exc_handler is None:
            self.default_exception_handler(context)
        else:
            _ = self._exc_handler(self, context)

    @override
    async def shutdown_asyncgens(self):
        pass

    @override
    async def shutdown_default_executor(self):
        pass

    def _timer_handle_cancelled(self, _th: TimerHandle) -> None:
        pass

    def _context_with_task_id(
        self, context: Context | None, task_id: bytes | None = None
    ) -> Context:
        base = context.copy() if context is not None else contextvars.copy_context()
        if task_id is None:
            task_id = self._generate_id()
        _ = base.run(_task_ctx.set, _TaskCtx(parent_id=task_id))
        return base


def _mix_id(a: bytes, b: int) -> bytes:
    return blake2b(b.to_bytes(4, "little", signed=True) + a, digest_size=12).digest()


def create_loop(seed: bytes) -> EventLoop:
    return EventLoop(seed)  # type: ignore[abstract]


def create_op(params: object) -> OpFuture:
    loop = asyncio.get_running_loop()
    if not isinstance(loop, EventLoop):
        raise RuntimeError("must be called from duron event loop")
    return loop.create_op(params)
