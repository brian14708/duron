from __future__ import annotations

from typing import TYPE_CHECKING
from typing_extensions import NamedTuple, ParamSpec, TypeVar, overload

if TYPE_CHECKING:
    from collections.abc import Callable


_T_co = TypeVar("_T_co", covariant=True)
_P = ParamSpec("_P")


class Reducer(NamedTuple):
    """Annotation to mark a parameter as a reducer."""

    reducer: Callable[[object, object], object]


@overload
def effect(fn: Callable[_P, _T_co], /) -> Callable[_P, _T_co]: ...
@overload
def effect() -> Callable[[Callable[_P, _T_co]], Callable[_P, _T_co]]: ...
def effect(
    fn: Callable[_P, _T_co] | None = None, /
) -> Callable[_P, _T_co] | Callable[[Callable[_P, _T_co]], Callable[_P, _T_co]]:
    """Decorator to mark async functions as effects.

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

    def decorate(fn: Callable[_P, _T_co]) -> Callable[_P, _T_co]:
        return fn

    return decorate
