from __future__ import annotations

from typing import TYPE_CHECKING, Final, Protocol
from typing_extensions import Self, final

if TYPE_CHECKING:
    from types import TracebackType


class Span(Protocol):
    id: str


@final
class _NullSpan:
    __slots__: tuple[str, ...] = ()

    id: str = "0000000000000000"

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return


NULL_SPAN: Final = _NullSpan()
