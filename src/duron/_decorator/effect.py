"""Effect function decorators for side effects in durable workflows.

This module provides the `@effect` decorator which wraps side-effecting
async functions. Effects:
- Run once per unique input during replay
- Record their return values to the log
- Support checkpointing for streaming operations (AsyncGenerator)
"""

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

from duron.typing import Unspecified, inspect_function

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from duron.typing import TypeHint


_T = TypeVar("_T")
_S = TypeVar("_S")
_T_co = TypeVar("_T_co", covariant=True)
_P = ParamSpec("_P")


@final
class CheckpointFn(Generic[_P, _S, _T]):
    def __init__(
        self,
        fn: Callable[Concatenate[_S, _P], AsyncGenerator[_T, _S]],
        initial: Callable[[], _S],
        reducer: Callable[[_S, _T], _S],
        action_type: TypeHint[_T],
        return_type: TypeHint[_S],
    ) -> None:
        self.fn = fn
        self.initial = initial
        self.reducer = reducer
        self.action_type = action_type
        self.return_type = return_type
        functools.update_wrapper(self, fn)

    def __call__(
        self,
        state: _S,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> AsyncGenerator[_T, _S]:
        return self.fn(state, *args, **kwargs)


@final
class EffectFn(Generic[_P, _T_co]):
    def __init__(
        self,
        fn: Callable[_P, Coroutine[Any, Any, _T_co]],
        return_type: TypeHint[Any],
    ) -> None:
        self.fn = fn
        self.return_type = return_type
        functools.update_wrapper(self, fn)

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
    """Decorator to mark async functions as effects.

    Effects are operations that interact with the outside world.

    Args:
        checkpoint: Enable checkpoint mode for async generators
        initial: Factory function for initial state (required with `checkpoint=True`)
        reducer: Function to reduce actions into state (required with `checkpoint=True`)

    Example:
        Basic example:
        ```python
        @duron.effect
        async def fetch_data(url: str) -> dict:
            return await http_client.get(url)
        ```

        Checkpointing example:
        ```python
        @duron.effect(checkpoint=True, initial=lambda: 0, reducer=int.__add__)
        async def count_items(state: int, items: list) -> AsyncGenerator[int, int]:
            # restore based on `state`
            for item in items:
                yield 1
        ```

    Returns:
        Function wrapper that can be invoked with [ctx.run()][duron.Context.run]

    Raises:
        ValueError: If checkpoint is True but initial or reducer is not provided.
    """
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
