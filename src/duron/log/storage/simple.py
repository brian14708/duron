from __future__ import annotations

import asyncio
import json
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, cast

from typing_extensions import override

from .base import BaseLogStorage, Lease, Offset

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from duron.log.entry import UnknownEntry

    from ..entry import Entry


class FileLogStorage(BaseLogStorage):
    _log_file: Path
    _leases: Lease | None
    _lock: asyncio.Lock

    def __init__(self, log_file: str | Path):
        self._log_file = Path(log_file)
        self._log_file.parent.mkdir(parents=True, exist_ok=True)
        self._leases = None
        self._lock = asyncio.Lock()

    @override
    async def stream(
        self, start: Offset | None, live: bool
    ) -> AsyncGenerator[tuple[Offset, Entry | UnknownEntry], None]:
        if not self._log_file.exists():
            return

        start_offset: int = int.from_bytes(start, "little") if start is not None else 0

        with open(self._log_file, "rb") as f:
            # Seek to start offset
            _ = f.seek(start_offset)

            # Read existing lines from start offset
            while True:
                line_start_offset = f.tell()
                line = f.readline()
                if line:
                    try:
                        entry = json.loads(line.decode().strip())
                        if isinstance(entry, dict):
                            yield (
                                Offset(line_start_offset.to_bytes(8, "little")),
                                cast("UnknownEntry", cast("object", entry)),
                            )
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        pass
                else:
                    # Reached end of file
                    break

            # If live mode, continue tailing
            if live:
                while True:
                    line_start_offset = f.tell()
                    line = f.readline()
                    if line:
                        try:
                            entry = json.loads(line.decode().strip())
                            if isinstance(entry, dict):
                                yield (
                                    Offset(line_start_offset.to_bytes(8, "little")),
                                    cast("UnknownEntry", cast("object", entry)),
                                )
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            pass
                    else:
                        await asyncio.sleep(0.1)

    @override
    async def acquire_lease(self) -> Lease:
        lease_id = Lease(uuid.uuid4().bytes)
        async with self._lock:
            self._leases = lease_id
        return lease_id

    @override
    async def release_lease(self, token: Lease) -> None:
        async with self._lock:
            if token == self._leases:
                self._leases = None

    @override
    async def append(self, token: Lease, entry: Entry) -> Offset:
        async with self._lock:
            if token != self._leases:
                raise ValueError("Invalid lease token")

            with open(self._log_file, "a") as f:
                entry_offset = f.tell()
                json.dump(entry, f, separators=(",", ":"))
                _ = f.write("\n")

            return Offset(entry_offset.to_bytes(8, "little"))


class MemoryLogStorage(BaseLogStorage):
    _entries: list[Entry]
    _leases: Lease | None
    _lock: asyncio.Lock
    _condition: asyncio.Condition

    def __init__(self, entries: list[Entry] | None = None):
        self._entries = entries or []
        self._leases = None
        self._lock = asyncio.Lock()
        self._condition = asyncio.Condition(self._lock)

    @override
    async def stream(
        self, start: Offset | None, live: bool
    ) -> AsyncGenerator[tuple[Offset, Entry | UnknownEntry], None]:
        start_index: int = int.from_bytes(start, "little") if start is not None else 0

        # Yield existing entries
        async with self._lock:
            entries_snapshot = self._entries.copy()

        for index in range(start_index, len(entries_snapshot)):
            yield (
                Offset(index.to_bytes(8, "little")),
                entries_snapshot[index],
            )

        # If live mode, continue monitoring for new entries
        if live:
            last_seen_index = len(entries_snapshot) - 1
            while True:
                async with self._condition:
                    # Wait for new entries or timeout
                    while len(self._entries) <= last_seen_index + 1:
                        _ = await self._condition.wait()

                    current_length = len(self._entries)

                for index in range(last_seen_index + 1, current_length):
                    yield (
                        Offset(index.to_bytes(8, "little")),
                        self._entries[index],
                    )
                    last_seen_index = index

    @override
    async def acquire_lease(self) -> Lease:
        lease_id = Lease(uuid.uuid4().bytes)
        async with self._lock:
            self._leases = lease_id
        return lease_id

    @override
    async def release_lease(self, token: Lease) -> None:
        async with self._lock:
            if token == self._leases:
                self._leases = None

    @override
    async def append(self, token: Lease, entry: Entry) -> Offset:
        async with self._condition:
            if token != self._leases:
                raise ValueError("Invalid lease token")

            index = len(self._entries)
            self._entries.append(entry)
            self._condition.notify_all()

            return Offset(index.to_bytes(8, "little"))

    async def entries(self) -> list[Entry]:
        async with self._lock:
            return self._entries.copy()
