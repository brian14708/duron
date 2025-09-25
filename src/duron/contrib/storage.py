from __future__ import annotations

import asyncio
import json
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, cast

from typing_extensions import override

from duron.log import LogStorage

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from duron.log import AnyEntry, Entry


class FileLogStorage(LogStorage[int, str]):
    _log_file: Path
    _leases: str | None
    _lock: asyncio.Lock

    def __init__(self, log_file: str | Path):
        self._log_file = Path(log_file)
        self._log_file.parent.mkdir(parents=True, exist_ok=True)
        self._leases = None
        self._lock = asyncio.Lock()

    @override
    async def stream(
        self, start: int | None, live: bool
    ) -> AsyncGenerator[tuple[int, AnyEntry], None]:
        if not self._log_file.exists():
            return

        start_offset: int = start if start is not None else 0

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
                                line_start_offset,
                                cast("AnyEntry", cast("object", entry)),
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
                                    line_start_offset,
                                    cast("AnyEntry", cast("object", entry)),
                                )
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            pass
                    else:
                        await asyncio.sleep(0.1)

    @override
    async def acquire_lease(self) -> str:
        lease_id = str(uuid.uuid4())
        async with self._lock:
            self._leases = lease_id
        return lease_id

    @override
    async def release_lease(self, token: str) -> None:
        async with self._lock:
            if token == self._leases:
                self._leases = None

    @override
    async def append(self, token: str, entry: Entry):
        async with self._lock:
            if token != self._leases:
                raise ValueError("Invalid lease token")

            with open(self._log_file, "a") as f:
                json.dump(entry, f, separators=(",", ":"))
                _ = f.write("\n")

    @override
    async def flush(self, token: str):
        pass


class MemoryLogStorage(LogStorage[int, str]):
    _entries: list[AnyEntry]
    _leases: str | None
    _lock: asyncio.Lock
    _condition: asyncio.Condition

    def __init__(self, entries: list[AnyEntry] | None = None):
        self._entries = entries or []
        self._leases = None
        self._lock = asyncio.Lock()
        self._condition = asyncio.Condition(self._lock)

    @override
    async def stream(
        self, start: int | None, live: bool
    ) -> AsyncGenerator[tuple[int, AnyEntry], None]:
        start_index: int = start if start is not None else 0

        # Yield existing entries
        async with self._lock:
            entries_snapshot = self._entries.copy()

        for index in range(start_index, len(entries_snapshot)):
            yield (
                index,
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
                        index,
                        self._entries[index],
                    )
                    last_seen_index = index

    @override
    async def acquire_lease(self) -> str:
        lease_id = str(uuid.uuid4())
        async with self._lock:
            self._leases = lease_id
        return lease_id

    @override
    async def release_lease(self, token: str) -> None:
        async with self._lock:
            if token == self._leases:
                self._leases = None

    @override
    async def append(self, token: str, entry: Entry):
        async with self._condition:
            if token != self._leases:
                raise ValueError("Invalid lease token")

            self._entries.append(cast("AnyEntry", cast("object", entry)))
            self._condition.notify_all()

    @override
    async def flush(self, token: str):
        pass

    async def entries(self) -> list[AnyEntry]:
        async with self._lock:
            return self._entries.copy()
