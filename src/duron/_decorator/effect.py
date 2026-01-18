from __future__ import annotations

import asyncio
import functools
import inspect
from typing import TYPE_CHECKING, Any, cast
from typing_extensions import NamedTuple, ParamSpec, TypeVar, overload

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Coroutine


_T = TypeVar("_T")
_P = ParamSpec("_P")


class Reducer(NamedTuple):
    """Annotation to mark a parameter as a reducer."""

    reducer: Callable[[object, object], object]


def _check_loop() -> None:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return

    from duron.loop import EventLoop  # noqa: PLC0415

    if isinstance(loop, EventLoop):
        msg = (
            "Effects cannot be called from within a duron EventLoop. "
            "Use 'ctx.run()' to execute effects."
        )
        raise RuntimeError(msg)  # noqa: TRY004


def _wrap_effect(fn: Callable[_P, Coroutine[Any, Any, _T] | _T]) -> Callable[_P, Any]:
    if inspect.iscoroutinefunction(fn):

        @functools.wraps(fn)
        async def async_wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _T:
            _check_loop()
            return cast("_T", await fn(*args, **kwargs))

        return async_wrapper
    if inspect.isasyncgenfunction(fn):

        @functools.wraps(fn)
        async def async_gen_wrapper(
            *args: _P.args, **kwargs: _P.kwargs
        ) -> AsyncGenerator[Any, Any]:
            _check_loop()
            gen = fn(*args, **kwargs)
            try:
                value = await anext(gen)
                while True:
                    try:
                        sent = yield value
                        value = await gen.asend(sent)
                    except GeneratorExit:  # noqa: PERF203
                        await gen.aclose()
                        raise
            except StopAsyncIteration:
                return

        return async_gen_wrapper

    @functools.wraps(fn)
    def sync_wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _T:
        _check_loop()
        return cast("_T", fn(*args, **kwargs))

    return sync_wrapper


@overload
def effect(fn: Callable[_P, _T], /) -> Callable[_P, _T]: ...
@overload
def effect() -> Callable[[Callable[_P, _T]], Callable[_P, _T]]: ...
def effect(
    fn: Callable[_P, _T] | None = None, /
) -> Callable[_P, _T] | Callable[[Callable[_P, _T]], Callable[_P, _T]]:
    """Mark async function as effect.

    Effects are operations that interact with the outside world.

    Example:
        ```python
        @duron.effect
        async def send_email(to: str, subject: str, body: str) -> None:
            # Send an email
            ...


        @duron.effect
        async def counter(
            state: Annotated[int, duron.Reducer(lambda s, a: s + a)], increment: int
        ) -> AsyncGenerator[int, int]:
            state += increment
            yield state
        ```


    Returns:
        Function wrapper that can be invoked with [ctx.run()][duron.Context.run]

    """
    if fn is not None:
        return _wrap_effect(fn)
    return _wrap_effect
