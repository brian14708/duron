from __future__ import annotations

from typing import TYPE_CHECKING, Generic, TypeVar
from typing_extensions import Any, override

if TYPE_CHECKING:
    from collections.abc import Mapping

_K = TypeVar("_K")
_V = TypeVar("_V")

_EMPTY: dict[Any, Any] = {}


class LinkedDict(Generic[_K, _V]):
    __slots__ = ("_cache", "_current", "_parent")

    def __init__(
        self,
        mapping: Mapping[_K, _V] | None = None,
        parent: LinkedDict[_K, _V] | None = None,
    ) -> None:
        self._current: dict[_K, _V] = dict(mapping) if mapping else _EMPTY
        self._parent = parent
        self._cache: dict[_K, _V] | None = None

    def extend(self, mapping: Mapping[_K, _V] | None) -> LinkedDict[_K, _V]:
        if not mapping:
            return self
        return self.__class__(mapping, self)

    def materialize(self) -> dict[_K, _V]:
        if self._cache is not None:
            return self._cache

        if self._parent is None:
            merged = dict(self._current)
        else:
            merged = self._parent.materialize().copy()
            merged.update(self._current)

        self._cache = merged
        return merged

    @override
    def __repr__(self) -> str:
        cls = self.__class__.__name__
        return f"{cls}({self.materialize()!r})"
