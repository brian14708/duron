from __future__ import annotations

from typing import TYPE_CHECKING, Generic, Protocol

from typing_extensions import TypeVar

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from duron.log.entry import AnyEntry

    from ..entry import Entry


__all__ = ["LogStorage"]

_TOffset = TypeVar("_TOffset")
_TLease = TypeVar("_TLease")


class LogStorage(Protocol, Generic[_TOffset, _TLease]):
    def stream(
        self, start: _TOffset | None, live: bool
    ) -> AsyncGenerator[tuple[_TOffset, AnyEntry], None]: ...

    async def acquire_lease(self) -> _TLease: ...

    async def release_lease(self, token: _TLease): ...

    async def append(self, token: _TLease, entry: Entry): ...

    async def flush(self, token: _TLease): ...
