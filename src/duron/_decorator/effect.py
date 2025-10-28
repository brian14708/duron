from __future__ import annotations

from typing import TYPE_CHECKING
from typing_extensions import NamedTuple, ParamSpec, TypeVar, overload

if TYPE_CHECKING:
    from collections.abc import Callable


_T = TypeVar("_T")
_P = ParamSpec("_P")


class Reducer(NamedTuple):
    """Annotation to mark a parameter as a reducer."""

    reducer: Callable[[object, object], object]


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
        return fn

    def decorate(fn: Callable[_P, _T]) -> Callable[_P, _T]:
        return fn

    return decorate
