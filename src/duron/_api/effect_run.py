"""Scoped effect runs and binding declared ports to invocation capabilities."""

# pyright: reportPrivateUsage=false
# ruff: file-ignore[private-member-access]

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, cast, overload
from typing_extensions import TypeVar

from duron._api.ports import Input, Output, PortReader, _check_port
from duron._runtime.ops import OwnedChannelExit, create_op

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Generator
    from types import TracebackType

    from duron._api.effect import Effect
    from duron._api.ports import Reader, Sender, _Port
    from duron._api.run import OutputCursor
    from duron._execution.handles import CommandSender
    from duron._runtime.api_context import WorkflowContextImpl
    from duron._runtime.channels import Stream

_T = TypeVar("_T")
_V = TypeVar("_V")


class EffectRun(Generic[_T]):
    """A scoped logical effect call; awaiting it observes its final result."""

    def __init__(
        self,
        context: WorkflowContextImpl,
        effect: Effect[..., _T],
        args: tuple[object, ...],
        kwargs: dict[str, object],
    ) -> None:
        self._context = context
        self._effect = effect
        self._args = args
        self._kwargs = kwargs
        self._stack = AsyncExitStack()
        self._task: asyncio.Task[_T] | None = None
        self._bindings: dict[_Port, object] = {}
        self._active = False
        self._entered = False
        self._observed = False

    @overload
    def port(self, declaration: Input[_V], /) -> Sender[_V]: ...
    @overload
    def port(self, declaration: Output[_V], /) -> Reader[_V]: ...
    def port(self, declaration: _Port, /) -> Sender[Any] | Reader[Any]:
        self._check()
        _check_port(declaration, self._effect.ports, self._effect.name)
        endpoint = self._bindings.get(declaration)
        if endpoint is None:
            msg = "effect run ports are available only after entering its scope"
            raise RuntimeError(msg)
        return cast("Sender[Any] | Reader[Any]", endpoint)

    def _check(self) -> None:
        if not self._active:
            msg = "effect run is outside its opening scope"
            raise RuntimeError(msg)

    async def __aenter__(self) -> EffectRun[_T]:
        if self._entered:
            msg = "an effect run can only be entered once"
            raise RuntimeError(msg)
        self._entered = True
        self._active = True
        bindings: dict[_Port, object] = {}
        handles: dict[str, object] = {}
        try:
            for port in self._effect.ports:
                if isinstance(port, Input):
                    commands = await self._stack.enter_async_context(
                        self._context._open_commands(port.item_type)
                    )
                    handles[f"_duron_port_{port.name}"] = commands.receiver
                    bindings[port] = _ScopedSender(self, commands.sender)
                else:
                    stream = await self._stack.enter_async_context(
                        self._context._open_stream(port.item_type)
                    )
                    handles[f"_duron_port_{port.name}"] = stream.sender
                    bindings[port] = _ScopedReader(self, stream)
            self._bindings = bindings
            bound = self._effect.signature.bind(*self._args, **self._kwargs)
            # Bind positional/keyword arguments before appending private wire ports.
            wire_kwargs = bound.kwargs | handles

            async def execute() -> _T:
                operation = self._context._prepare(
                    self._effect, tuple(bound.args), wire_kwargs
                )
                return await self._context._ctx.run_effect(
                    operation, self._effect.return_type
                )

            self._task = asyncio.create_task(execute())
            # Admit the effect before user code can interact with its endpoints.
            await asyncio.sleep(0)
            if self._task.done():
                self._task.result()
        except BaseException:
            self._active = False
            if self._task is not None:
                self._task.cancel()
                await asyncio.gather(self._task, return_exceptions=True)
            await self._stack.aclose()
            raise
        else:
            return self

    def __await__(self) -> Generator[Any, None, _T]:
        return self._result().__await__()

    async def _result(self) -> _T:
        self._check()
        assert self._task is not None
        try:
            result = await asyncio.shield(self._task)
        except Exception:
            self._observed = True
            raise
        self._observed = True
        return result

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        task = self._task
        failure: BaseException | None = None
        try:
            if task is not None:
                cancelled_by_scope = not task.done()
                if exc_type is None and not self._observed:
                    # Preserve whether normal exit cancelled unfinished work or
                    # observed a completed result, independent of replay timing.
                    def still_running() -> bool:
                        return not task.done()

                    cancelled_by_scope = await self._context._ctx.run(still_running)
                if cancelled_by_scope:
                    task.cancel()
                outcome = (await asyncio.gather(task, return_exceptions=True))[0]
                if (
                    not cancelled_by_scope
                    and isinstance(outcome, BaseException)
                    and not self._observed
                ):
                    failure = outcome
        finally:
            try:
                await self._stack.aclose()
            finally:
                self._active = False
        if exc_type is None and failure is not None:
            raise failure


@dataclass(frozen=True, slots=True, eq=False)
class _ScopedSender(Generic[_T]):
    run: EffectRun[Any]
    sender: CommandSender[_T]

    async def send(self, value: _T, *, idempotency_key: str | None = None) -> None:
        self.run._check()
        await self.sender.send(value, idempotency_key=idempotency_key)

    async def close(self) -> None:
        self.run._check()
        await create_op(
            self.run._context._ctx.loop,
            OwnedChannelExit(self.sender._reference.channel_id),
        )


@dataclass(frozen=True, slots=True, eq=False)
class _ScopedReader(Generic[_T]):
    run: EffectRun[Any]
    stream: Stream[_T]

    def reader(
        self, *, after: OutputCursor | None = None, prefetch: int = 64
    ) -> PortReader[_T]:
        self.run._check()
        if type(prefetch) is not int or prefetch <= 0:
            msg = "prefetch must be a positive integer"
            raise ValueError(msg)
        return _ScopedIterator(
            self.run, self.stream.reader(prefetch=prefetch, after=after)
        )

    def __aiter__(self) -> AsyncIterator[_T]:
        return _ScopedIterator(self.run, aiter(self.stream))


@dataclass(frozen=True, slots=True, eq=False)
class _ScopedIterator(Generic[_T]):
    run: EffectRun[Any]
    reader: PortReader[_T]

    @property
    def cursor(self) -> OutputCursor:
        return self.reader.cursor

    def __aiter__(self) -> _ScopedIterator[_T]:
        return self

    async def __anext__(self) -> _T:
        self.run._check()
        return await anext(self.reader)
