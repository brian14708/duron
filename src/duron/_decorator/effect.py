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

from duron.typing import Unspecified, inspect_function

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from duron.typing import TypeHint


_T = TypeVar("_T")
_S = TypeVar("_S")
_T_co = TypeVar("_T_co", covariant=True)
_P = ParamSpec("_P")


@dataclass(slots=True)
class CheckpointFn(Generic[_P, _S, _T]):
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
class EffectFn(Generic[_P, _T_co]):
    fn: Callable[_P, Coroutine[Any, Any, _T_co]]
    return_type: TypeHint[Any]

    def __call__(
        self,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> Coroutine[Any, Any, _T_co]:
        return self.fn(*args, **kwargs)


@overload
def effect(
    fn: Callable[_P, Coroutine[Any, Any, _T_co]],
    /,
) -> EffectFn[_P, _T_co]: ...
@overload
def effect(
    *,
    checkpoint: Literal[False] = ...,
) -> Callable[
    [Callable[_P, Coroutine[Any, Any, _T_co]]],
    EffectFn[_P, _T_co],
]: ...
@overload
def effect(
    *,
    checkpoint: Literal[True],
    initial: Callable[[], _S],
    reducer: Callable[[_S, _T], _S],
) -> Callable[
    [Callable[Concatenate[_S, _P], AsyncGenerator[_T, _S]]],
    CheckpointFn[_P, _S, _T],
]: ...
def effect(
    fn: Callable[_P, Coroutine[Any, Any, _T_co]] | None = None,
    /,
    *,
    # checkpoint parameters
    checkpoint: bool = False,
    initial: Callable[[], _S] | None = None,
    reducer: Callable[[_S, _T], _S] | None = None,
) -> (
    EffectFn[_P, _T_co]
    | Callable[
        [Callable[_P, Coroutine[Any, Any, _T_co]]],
        EffectFn[_P, _T_co],
    ]
    | Callable[
        [Callable[Concatenate[_S, _P], AsyncGenerator[_T, _S]]],
        CheckpointFn[_P, _S, _T],
    ]
):
    if fn is not None:
        return EffectFn(
            fn=fn,
            return_type=inspect_function(fn).return_type,
        )

    if checkpoint:
        if not initial or not reducer:
            msg = "initial and reducer must be provided for checkpoint ops"
            raise ValueError(msg)

        def decorate_ckpt(
            fn: Callable[Concatenate[_S, _P], AsyncGenerator[_T, _S]],
        ) -> CheckpointFn[_P, _S, _T]:
            return_type: TypeHint[_S] = Unspecified
            action_type: TypeHint[_T] = Unspecified
            ret = inspect_function(fn).return_type
            if get_origin(ret) is AsyncGenerator:
                action_type, return_type = get_args(ret)
            return CheckpointFn(
                fn=fn,
                initial=initial,
                reducer=reducer,
                action_type=action_type,
                return_type=return_type,
            )

        return decorate_ckpt

    def decorate(
        fn: Callable[_P, Coroutine[Any, Any, _T_co]],
    ) -> EffectFn[_P, _T_co]:
        return EffectFn(
            fn=fn,
            return_type=inspect_function(fn).return_type,
        )

    return decorate
