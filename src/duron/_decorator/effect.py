from __future__ import annotations

import functools
from collections.abc import AsyncGenerator
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
from typing_extensions import final

from duron.typing import UnspecifiedType, inspect_function

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from duron.typing import TypeHint


_T = TypeVar("_T")
_S = TypeVar("_S")
_T_co = TypeVar("_T_co", covariant=True)
_P = ParamSpec("_P")


@final
class EffectFn(Generic[_P, _T_co]):
    def __init__(
        self,
        fn: Callable[_P, Coroutine[Any, Any, _T_co]],
    ) -> None:
        self.fn = fn
        self.type_hint = inspect_function(fn)
        functools.update_wrapper(self, fn)

    def __call__(
        self, *args: _P.args, **kwargs: _P.kwargs
    ) -> Coroutine[Any, Any, _T_co]:
        return self.fn(*args, **kwargs)


@final
class StatefulFn(Generic[_P, _S, _T]):
    def __init__(
        self,
        fn: Callable[Concatenate[_S, _P], AsyncGenerator[_T, _S]],
        initial: Callable[[], _S],
        reducer: Callable[[_S, _T], _S],
    ) -> None:
        self.fn = fn
        self.type_hint = inspect_function(fn)
        self.initial = initial
        self.reducer = reducer

        action_type: TypeHint[_T] = UnspecifiedType
        if get_origin(ret := self.type_hint.return_type) is AsyncGenerator:
            action_type, _ = get_args(ret)
        self.action_type = action_type
        functools.update_wrapper(self, fn)

    def __call__(
        self, state: _S, *args: _P.args, **kwargs: _P.kwargs
    ) -> AsyncGenerator[_T, _S]:
        return self.fn(state, *args, **kwargs)


@overload
def effect(fn: Callable[_P, Coroutine[Any, Any, _T_co]], /) -> EffectFn[_P, _T_co]: ...
@overload
def effect(
    *, stateful: Literal[False] = ...
) -> Callable[[Callable[_P, Coroutine[Any, Any, _T_co]]], EffectFn[_P, _T_co]]: ...
@overload
def effect(
    *,
    stateful: Literal[True],
    initial: Callable[[], _S],
    reducer: Callable[[_S, _T], _S],
) -> Callable[
    [Callable[Concatenate[_S, _P], AsyncGenerator[_T, _S]]], StatefulFn[_P, _S, _T]
]: ...
def effect(
    fn: Callable[_P, Coroutine[Any, Any, _T_co]] | None = None,
    /,
    *,
    # stateful parameters
    stateful: bool = False,
    initial: Callable[[], _S] | None = None,
    reducer: Callable[[_S, _T], _S] | None = None,
) -> (
    EffectFn[_P, _T_co]
    | Callable[[Callable[_P, Coroutine[Any, Any, _T_co]]], EffectFn[_P, _T_co]]
    | Callable[
        [Callable[Concatenate[_S, _P], AsyncGenerator[_T, _S]]], StatefulFn[_P, _S, _T]
    ]
):
    """Decorator to mark async functions as effects.

    Effects are operations that interact with the outside world.

    Args:
        stateful: Whether the effect is stateful.
        initial: Factory function for initial state (required with `stateful=True`)
        reducer: Function to reduce actions into state (required with `stateful=True`)

    Example:
        Basic example:
        ```python
        @duron.effect
        async def fetch_data(url: str) -> dict:
            return await http_client.get(url)
        ```

        Stateful example:
        ```python
        @duron.effect(stateful=True, initial=lambda: 0, reducer=int.__add__)
        async def count_items(state: int, items: list) -> AsyncGenerator[int, int]:
            # restore based on `state`
            for item in items:
                yield 1
        ```

    Returns:
        Function wrapper that can be invoked with [ctx.run()][duron.Context.run]

    Raises:
        ValueError: If stateful is True but initial or reducer is not provided.
    """
    if fn is not None:
        return EffectFn(fn)

    if stateful:
        if not initial or not reducer:
            msg = "initial and reducer must be provided for stateful ops"
            raise ValueError(msg)

        def decorate_stateful(
            fn: Callable[Concatenate[_S, _P], AsyncGenerator[_T, _S]],
        ) -> StatefulFn[_P, _S, _T]:
            return StatefulFn(fn, initial, reducer)

        return decorate_stateful

    return EffectFn
