from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, NewType

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from duron.log.entry import UnknownEntry

    from ..entry import Entry

Offset = NewType("Offset", bytes)
Lease = NewType("Lease", bytes)


class BaseLogStorage(ABC):
    @abstractmethod
    def stream(
        self, start: Offset | None, live: bool
    ) -> AsyncGenerator[tuple[Offset, Entry | UnknownEntry], None]: ...

    @abstractmethod
    async def acquire_lease(self) -> Lease: ...

    @abstractmethod
    async def release_lease(self, token: Lease): ...

    @abstractmethod
    async def append(self, token: Lease, entry: Entry) -> Offset: ...
