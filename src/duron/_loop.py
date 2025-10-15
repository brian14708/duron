from __future__ import annotations

import asyncio
import contextlib
import contextvars
import logging
from asyncio import events, tasks
from collections import deque
from dataclasses import dataclass
from hashlib import blake2b
from heapq import heappop, heappush
from typing import TYPE_CHECKING, Generic
from typing_extensions import (
    Any,
    TypeVar,
    TypeVarTuple,
    Unpack,
    overload,
    override,
)

from duron.log import derive_id, random_id

_T = TypeVar("_T")

if TYPE_CHECKING:
    import sys
    from asyncio.futures import Future
    from collections.abc import Callable, Coroutine, Generator
    from contextvars import Context

    _Ts = TypeVarTuple("_Ts")

    if sys.version_info >= (3, 12):
        _TaskCompatibleCoro = Coroutine[Any, Any, _T]
    else:
        _TaskCompatibleCoro = Generator[Any, None, _T] | Coroutine[Any, Any, _T]


logger = logging.getLogger(__name__)


_task_ctx: contextvars.ContextVar[_TaskCtx] = contextvars.ContextVar("duron.task")


class OpFuture(asyncio.Future[_T], Generic[_T]):
    __slots__: tuple[str, ...] = ("id", "params")

    def __init__(
        self,
        id_: str,
        params: object,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        super().__init__(loop=loop)
        self.id = id_
        self.params = params


@dataclass(slots=True)
class WaitSet:
    ops: list[OpFuture[object]]
    timer: int | None
    event: asyncio.Event

    async def block(self, now_us: int, max_timeout_us: int = -1) -> None:
        if self.timer is None and max_timeout_us < 0:
            _ = await self.event.wait()
            return

        if self.timer is None or (
            max_timeout_us > 0 and (self.timer - now_us) > max_timeout_us
        ):
            t = max_timeout_us / 1e6
        else:
            t = (self.timer - now_us) / 1e6
        if t > 0:
            with contextlib.suppress(asyncio.TimeoutError):
                _ = await asyncio.wait_for(self.event.wait(), timeout=t)


@dataclass(slots=True)
class _TaskCtx:
    parent_id: str
    seq: int = 0


class EventLoop(asyncio.AbstractEventLoop):
    __slots__: tuple[str, ...] = (
        "_closed",
        "_ctx",
        "_event",
        "_exc_handler",
        "_host",
        "_now_us",
        "_ops",
        "_ready",
        "_timers",
    )

    def __init__(self, host: asyncio.AbstractEventLoop) -> None:
        self._ready: deque[asyncio.Handle] = deque()
        self._host: asyncio.AbstractEventLoop = host
        self._exc_handler: (
            Callable[[asyncio.AbstractEventLoop, dict[str, object]], object] | None
        ) = None
        self._ops: dict[str, OpFuture[object]] = {}
        self._ctx: _TaskCtx = _TaskCtx(parent_id="")
        self._key: bytes = b""
        self._now_us: int = 0
        self._closed: bool = False
        self._event: asyncio.Event = asyncio.Event()  # loop = _host
        self._timers: list[asyncio.TimerHandle] = []

    def set_key(self, key: bytes) -> None:
        # Derive a fixed-length key from the provided key
        self._key = blake2b(key, digest_size=16, key=self._key).digest()

    def generate_op_id(self) -> str:
        ctx = _task_ctx.get(self._ctx)
        ctx.seq += 1
        return derive_id(
            ctx.parent_id, context=(ctx.seq - 1).to_bytes(4, "big"), key=self._key
        )

    @override
    def call_soon(
        self,
        callback: Callable[[Unpack[_Ts]], object],
        *args: Unpack[_Ts],
        context: Context | None = None,
    ) -> asyncio.Handle:
        h = asyncio.Handle(
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
    ) -> asyncio.TimerHandle:
        th = asyncio.TimerHandle(
            int(when * 1e6),
            callback,
            args,
            loop=self,
            context=context,
        )
        heappush(self._timers, th)
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
    ) -> asyncio.TimerHandle:
        th = asyncio.TimerHandle(
            self.time_us() + int(delay * 1e6),
            callback,
            args,
            loop=self,
            context=context,
        )
        heappush(self._timers, th)
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
        **kwargs: Any,
    ) -> asyncio.Task[_T]:
        token = _task_ctx.set(_TaskCtx(parent_id=self.generate_op_id()))
        task = asyncio.Task(
            coro,
            loop=self,
            **kwargs,
        )
        _task_ctx.reset(token)
        return task

    def poll_completion(self, task: Future[_T]) -> WaitSet | None:
        now = self.time_us()
        self._event.clear()
        prev_task = tasks.current_task()
        if prev_task:
            tasks._leave_task(self._host, prev_task)  # noqa: SLF001
        events._set_running_loop(self)  # noqa: SLF001
        try:
            timers = self._timers
            ready = self._ready
            while True:
                deadline: int | None = None
                while timers:
                    ht = timers[0]
                    t = int(ht.when())
                    if ht._cancelled:  # noqa: SLF001
                        _ = heappop(timers)
                    elif t <= now:
                        _ = heappop(timers)
                        ready.append(ht)
                    else:
                        deadline = t
                        break

                if not ready:
                    break
                while ready:
                    h = ready.popleft()
                    if h._cancelled:  # noqa: SLF001
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
                ops=[*self._ops.values()],
                timer=deadline,
                event=self._event,
            )
        finally:
            events._set_running_loop(self._host)  # noqa: SLF001
            if prev_task:
                tasks._enter_task(self._host, prev_task)  # noqa: SLF001

    def create_op(self, params: object, *, external: bool = False) -> OpFuture[object]:
        if external:
            id_ = random_id()
            self._event.set()
        else:
            id_ = self.generate_op_id()
        op_fut: OpFuture[object] = OpFuture(id_, params, self)
        self._ops[id_] = op_fut
        return op_fut

    @overload
    def post_completion(
        self,
        id_: str,
        *,
        result: object,
    ) -> None: ...
    @overload
    def post_completion(
        self,
        id_: str,
        *,
        exception: Exception | asyncio.CancelledError,
    ) -> None: ...
    def post_completion(
        self,
        id_: str,
        *,
        result: object = None,
        exception: Exception | asyncio.CancelledError | None = None,
    ) -> None:
        if op := self._ops.pop(id_, None):
            if op.done():
                return
            tid = derive_id(op.id, key=self._key)
            token = _task_ctx.set(_TaskCtx(parent_id=tid))
            if exception is None:
                _ = self.call_soon(op.set_result, result)
            elif type(exception) is asyncio.CancelledError:
                _ = self.call_soon(op.cancel)
            else:
                _ = self.call_soon(op.set_exception, exception)
            _task_ctx.reset(token)

    @override
    def is_closed(self) -> bool:
        return self._closed

    @override
    def close(self) -> None:
        while self._ops:
            ops, self._ops = self._ops, {}
            for op in ops.values():
                op.cancel()
                p = self.poll_completion(op)
                if p is not None:
                    logger.warning(
                        "Event loop closed with pending operations: %r", p.ops
                    )
        while self._timers:
            th = heappop(self._timers)
            th.cancel()
        while self._ready:
            h = self._ready.popleft()
            h.cancel()
        self._closed = True

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
    async def shutdown_default_executor(self) -> None:
        pass

    def _timer_handle_cancelled(self, _th: asyncio.TimerHandle) -> None:
        pass


def create_loop(
    parent_loop: asyncio.AbstractEventLoop,
) -> EventLoop:
    return EventLoop(parent_loop)  # type: ignore[abstract]


def _copy_future_state(source: asyncio.Future[_T], dest: asyncio.Future[_T]) -> None:
    assert source.done()  # noqa: S101
    if dest.cancelled():
        return
    assert not dest.done()  # noqa: S101
    if source.cancelled():
        _ = dest.cancel()
    elif (exception := source.exception()) is not None:
        dest.set_exception(exception)
    else:
        dest.set_result(source.result())


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
