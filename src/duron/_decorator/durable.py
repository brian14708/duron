"""The internal ``DurableFn`` carrier consumed by the runtime.

The public authoring surface is :func:`duron.workflow`, which builds a
``DurableFn`` directly. This module intentionally no longer exposes a
decorator, injection markers, or the old validation error type.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Concatenate, Generic
from typing_extensions import Any, ParamSpec, TypeVar, final

from duron.typing import UnspecifiedType, inspect_function

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from duron._core.context import Context
    from duron.codec import Codec
    from duron.typing import TypeHint


_T = TypeVar("_T")
_P = ParamSpec("_P")


@final
class DurableFn(Generic[_P, _T]):
    __slots__ = ("codec", "fn")

    def __init__(
        self,
        codec: Codec,
        fn: Callable[Concatenate[Context, _P], Coroutine[Any, Any, _T]],
    ) -> None:
        self.codec = codec
        self.fn = fn

    def positional_type(self, index: int) -> TypeHint[Any]:
        """Return the declared type of the ``index``-th user positional argument.

        The runtime injects the leading context parameter, so user-supplied
        positional argument ``i`` maps to declared parameter ``i + 1``.

        Returns:
            The declared type hint, or unspecified when out of range.

        """
        hint = inspect_function(self.fn)
        parameters = hint.parameters
        if index + 1 < len(parameters):
            return hint.parameter_types.get(parameters[index + 1], UnspecifiedType)
        return UnspecifiedType
