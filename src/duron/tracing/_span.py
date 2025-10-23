from __future__ import annotations

from typing import TYPE_CHECKING, Final, Literal
from typing_extensions import Protocol, Self, final

if TYPE_CHECKING:
    from types import TracebackType

    from duron.typing import JSONValue


class Span(Protocol):
    """Protocol for tracing spans that represent units of work."""

    def record(self, **kwargs: JSONValue) -> None:
        """Record an attribute on this span.

        Args:
            **kwargs: JSON-serializable value to record
        """
        ...

    def set_status(
        self, status: Literal["OK", "ERROR"], message: str | None = None, /
    ) -> None:
        """Set the status of this span.

        Args:
            status: Either "OK" for successful completion or "ERROR" for failure
            message: Optional status message, typically used with "ERROR" status
                    to provide error details
        """
        ...


@final
class _NullSpan:
    __slots__: tuple[str, ...] = ()

    def __enter__(self) -> Self:
        return self

    @staticmethod
    def record(**_kwargs: JSONValue) -> None:
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
