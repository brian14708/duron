from __future__ import annotations

from collections.abc import AsyncGenerator
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Concatenate,
    Generic,
    Literal,
    ParamSpec,
    TypeVar,
    get_args,
    get_origin,
    overload,
)

from duron.typing import inspect_function, unspecified

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

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
    return_type: TypeHint[_S]

    def __call__(
        self,
        state: _S,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> AsyncGenerator[_T, _S]:
        return self.fn(state, *args, **kwargs)


@dataclass(slots=True)
class Op(Generic[_P, _T_co]):
    fn: Callable[_P, Coroutine[Any, Any, _T_co]]
    return_type: TypeHint[Any]

    def __call__(
        self,
        *args: _P.args,
        **kwargs: _P.kwargs,
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
    checkpoint: Literal[False] = ...,
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
    [Callable[Concatenate[_S, _P], AsyncGenerator[_T, _S]]],
    CheckpointOp[_P, _S, _T],
]: ...
def op(
    fn: Callable[_P, Coroutine[Any, Any, _T_co]] | None = None,
    /,
    *,
    return_type: TypeHint[Any] = unspecified,
    # checkpoint parameters
    checkpoint: bool = False,
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
            action_type_local = action_type
            return_type_local = return_type
            if not action_type_local or not return_type_local:
                ret = inspect_function(fn).return_type
                if get_origin(ret) is AsyncGenerator:
                    yield_, send = get_args(ret)
                    if not action_type_local:
                        action_type_local = yield_
                    if not return_type_local:
                        return_type_local = send
            return CheckpointOp(
                fn=fn,
                initial=initial,
                reducer=reducer,
                action_type=action_type_local,
                return_type=return_type_local,
            )

        return decorate_ckpt

    def decorate(
        fn: Callable[_P, Coroutine[Any, Any, _T_co]],
    ) -> Op[_P, _T_co]:
        return Op(
            fn=fn,
            return_type=return_type or inspect_function(fn).return_type,
        )

    if fn is not None:
        return decorate(fn)
    return decorate
