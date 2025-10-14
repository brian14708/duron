from __future__ import annotations

from typing import Generic, TypeVar
from typing_extensions import Any, override

_K = TypeVar("_K")
_V = TypeVar("_V")

_EMPTY: dict[Any, Any] = {}


class LinkedDict(Generic[_K, _V]):
    __slots__ = ("_current", "_parent")

    def __init__(
        self,
        mapping: dict[_K, _V] | None = None,
        parent: LinkedDict[_K, _V] | None = None,
    ) -> None:
        self._current: dict[_K, _V] = mapping or _EMPTY
        self._parent = parent

    def extend(self, mapping: dict[_K, _V] | None) -> LinkedDict[_K, _V]:
        if not mapping:
            return self
        return LinkedDict(mapping, self)

    def materialize(self) -> dict[_K, _V]:
        if self._parent is None:
            return self._current.copy()

        merged = self._parent.materialize()
        merged.update(self._current)
        self._current = merged
        self._parent = None
        return merged

    def get(self, key: _K, default: _V | None = None) -> _V | None:
        if key in self._current:
            return self._current[key]
        if self._parent is not None:
            return self._parent.get(key, default)
        return default

    def assign_to(self, to_merge: dict[_K, _V]) -> None:
        if self._parent is not None:
            self._parent.assign_to(to_merge)

        if self._current is not _EMPTY:
            to_merge.update(self._current)

    @override
    def __repr__(self) -> str:
        cls = self.__class__.__name__
        return f"{cls}({self.materialize()!r})"
