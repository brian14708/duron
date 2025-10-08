from __future__ import annotations

from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Concatenate,
    Generic,
    Literal,
    ParamSpec,
    TypeVar,
    overload,
)

from duron.typing import unspecified

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Coroutine

    from duron.typing import TypeHint


_T = TypeVar("_T")
_S = TypeVar("_S")
_T_co = TypeVar("_T_co", covariant=True)
_P = ParamSpec("_P")


@dataclass(slots=True)
class CheckpointOp(Generic[_P, _S, _T]):
    fn: Callable[Concatenate[_S, _P], AsyncGenerator[_T, _S]]
    initial: Callable[[], _S]
    reducer: Callable[[_S, _T], _S]
    action_type: TypeHint[_T]
    state_type: TypeHint[_S]

    def __call__(
        self, state: _S, *args: _P.args, **kwargs: _P.kwargs
    ) -> AsyncGenerator[_T, _S]:
        return self.fn(state, *args, **kwargs)


@dataclass(slots=True)
class Op(Generic[_P, _T_co]):
    fn: Callable[_P, Coroutine[Any, Any, _T_co]]
    return_type: TypeHint[Any]

    def __call__(
        self, *args: _P.args, **kwargs: _P.kwargs
    ) -> Coroutine[Any, Any, _T_co]:
        return self.fn(*args, **kwargs)


@overload
def op(
    fn: Callable[_P, Coroutine[Any, Any, _T_co]],
    /,
) -> Op[_P, _T_co]: ...
@overload
def op(
    *,
    return_type: TypeHint[Any] = ...,
    checkpoint: Literal[False],
) -> Callable[
    [Callable[_P, Coroutine[Any, Any, _T_co]]],
    Op[_P, _T_co],
]: ...
@overload
def op(
    *,
    checkpoint: Literal[True],
    initial: Callable[[], _S],
    reducer: Callable[[_S, _T], _S],
    return_type: TypeHint[_S] = ...,
    action_type: TypeHint[_T] = ...,
) -> Callable[
    [Callable[Concatenate[_S, _P], AsyncGenerator[_T, _S]]], CheckpointOp[_P, _S, _T]
]: ...
def op(
    fn: Callable[_P, Coroutine[Any, Any, _T_co]] | None = None,
    /,
    *,
    return_type: TypeHint[Any] = unspecified,
    # checkpoint parameters
    checkpoint: Literal[True] | Literal[False] = False,
    initial: Callable[[], _S] | None = None,
    reducer: Callable[[_S, _T], _S] | None = None,
    action_type: TypeHint[_T] = unspecified,
) -> (
    Op[_P, _T_co]
    | Callable[
        [Callable[_P, Coroutine[Any, Any, _T_co]]],
        Op[_P, _T_co],
    ]
    | Callable[
        [Callable[Concatenate[_S, _P], AsyncGenerator[_T, _S]]],
        CheckpointOp[_P, _S, _T],
    ]
):
    if checkpoint and initial and reducer:

        def decorate_ckpt(
            fn: Callable[Concatenate[_S, _P], AsyncGenerator[_T, _S]],
        ) -> CheckpointOp[_P, _S, _T]:
            return CheckpointOp(
                fn=fn,
                initial=initial,
                reducer=reducer,
                action_type=action_type,
                state_type=return_type,
            )

        return decorate_ckpt

    def decorate(
        fn: Callable[_P, Coroutine[Any, Any, _T_co]],
    ) -> Op[_P, _T_co]:
        return Op(fn=fn, return_type=return_type)

    if fn is not None:
        return decorate(fn)
    else:
        return decorate
