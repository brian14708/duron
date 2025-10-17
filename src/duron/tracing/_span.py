from __future__ import annotations

from typing import TYPE_CHECKING, Final, Literal, Protocol
from typing_extensions import Self, final

if TYPE_CHECKING:
    from types import TracebackType

    from duron.typing import JSONValue


class Span(Protocol):
    def record(
        self,
        key: str,
        value: JSONValue,
        /,
    ) -> None: ...

    def set_status(
        self,
        status: Literal["OK", "ERROR"],
        message: str | None = None,
        /,
    ) -> None: ...


@final
class _NullSpan:
    __slots__: tuple[str, ...] = ()

    def __enter__(self) -> Self:
        return self

    @staticmethod
    def record(_key: str, _value: JSONValue) -> None:
        return

    @staticmethod
    def set_status(
        _status: Literal["OK", "ERROR"], _message: str | None = None
    ) -> None:
        return

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return


NULL_SPAN: Final = _NullSpan()
