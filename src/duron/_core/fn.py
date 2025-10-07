from __future__ import annotations

from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Concatenate,
    Generic,
    ParamSpec,
    TypeVar,
    final,
    overload,
)

from duron._core.config import config
from duron._core.job import Job

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Coroutine
    from contextlib import AbstractAsyncContextManager
    from types import TracebackType

    from duron._core.context import Context
    from duron.codec import Codec
    from duron.log import LogStorage


_T = TypeVar("_T")
_S = TypeVar("_S")
_T_co = TypeVar("_T_co", covariant=True)
_P = ParamSpec("_P")


@dataclass(slots=True)
class Fn(Generic[_P, _T_co]):
    codec: Codec
    fn: Callable[Concatenate[Context, _P], Coroutine[Any, Any, _T_co]]

    def __call__(
        self, ctx: Context, *args: _P.args, **kwargs: _P.kwargs
    ) -> Coroutine[Any, Any, _T_co]:
        return self.fn(ctx, *args, **kwargs)

    def create_job(
        self, log: LogStorage
    ) -> AbstractAsyncContextManager[Job[_P, _T_co]]:
        return _JobGuard(Job(self, log))


@final
class _JobGuard(Generic[_P, _T_co]):
    __slots__ = ("_job",)

    def __init__(self, job: Job[_P, _T_co]) -> None:
        self._job = job

    async def __aenter__(self) -> Job[_P, _T_co]:
        return self._job

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self._job.close()


@overload
def fn(
    _fn: Callable[Concatenate[Context, _P], Coroutine[Any, Any, _T_co]],
) -> Fn[_P, _T_co]: ...
@overload
def fn(
    *, codec: Codec | None = None
) -> Callable[
    [Callable[Concatenate[Context, _P], Coroutine[Any, Any, _T_co]]],
    Fn[_P, _T_co],
]: ...
def fn(
    _fn: Callable[Concatenate[Context, _P], Coroutine[Any, Any, _T_co]] | None = None,
    *,
    codec: Codec | None = None,
) -> (
    Fn[_P, _T_co]
    | Callable[
        [Callable[Concatenate[Context, _P], Coroutine[Any, Any, _T_co]]],
        Fn[_P, _T_co],
    ]
):
    """
    Make a function as durable, meaning its execution can be recorded and
    replayed.
    """

    def decorate(
        fn: Callable[Concatenate[Context, _P], Coroutine[Any, Any, _T_co]],
    ) -> Fn[_P, _T_co]:
        return Fn(codec=codec or config.codec, fn=fn)

    if _fn is not None:
        return decorate(_fn)
    else:
        return decorate


@dataclass(slots=True)
class CheckpointFn(Generic[_P, _S, _T]):
    fn: Callable[Concatenate[_S, _P], AsyncGenerator[_T, _S]]
    initial: Callable[[], _S]
    reducer: Callable[[_S, _T], _S]
    action_type: type[_T] | None
    state_type: type[_S] | None

    def __call__(
        self, state: _S, *args: _P.args, **kwargs: _P.kwargs
    ) -> AsyncGenerator[_T, _S]:
        return self.fn(state, *args, **kwargs)


def checkpoint(
    *,
    initial: Callable[[], _S],
    reducer: Callable[[_S, _T], _S],
    action_type: type[_T] | None = None,
    state_type: type[_S] | None = None,
) -> Callable[
    [Callable[Concatenate[_S, _P], AsyncGenerator[_T, _S]]],
    CheckpointFn[_P, _S, _T],
]:
    def decorate(
        fn: Callable[Concatenate[_S, _P], AsyncGenerator[_T, _S]],
    ) -> CheckpointFn[_P, _S, _T]:
        return CheckpointFn(
            fn=fn,
            initial=initial,
            reducer=reducer,
            action_type=action_type,
            state_type=state_type,
        )

    return decorate


@dataclass(slots=True)
class EffectFn(Generic[_P, _T_co]):
    fn: Callable[_P, _T_co]
    return_type: type[_T_co] | None = None

    def __call__(self, *args: _P.args, **kwargs: _P.kwargs) -> _T_co:
        return self.fn(*args, **kwargs)


@overload
def effect(
    _fn: Callable[_P, _T_co],
) -> EffectFn[_P, _T_co]: ...
@overload
def effect(
    *,
    return_type: type | None = ...,
) -> Callable[
    [Callable[_P, _T_co]],
    EffectFn[_P, _T_co],
]: ...
def effect(
    _fn: Callable[_P, _T_co] | None = None, *, return_type: type | None = None
) -> (
    EffectFn[_P, _T_co]
    | Callable[
        [Callable[_P, _T_co]],
        EffectFn[_P, _T_co],
    ]
):
    """
    Make a function as durable, meaning its execution can be recorded and
    replayed.
    """

    def decorate(
        fn: Callable[_P, _T_co],
    ) -> EffectFn[_P, _T_co]:
        return EffectFn(fn=fn, return_type=return_type)

    if _fn is not None:
        return decorate(_fn)
    else:
        return decorate
