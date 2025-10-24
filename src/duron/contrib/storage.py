from __future__ import annotations

import asyncio
import json
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from io import TextIOBase

    from duron.log import BaseEntry, Entry


try:
    import fcntl

    def lock_file(f: TextIOBase, /) -> None:
        if f.writable():
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def unlock_file(f: TextIOBase, /) -> None:
        if f.writable():
            fcntl.flock(f, fcntl.LOCK_UN)

except ModuleNotFoundError:

    def lock_file(_f: TextIOBase, /) -> None:
        pass

    def unlock_file(_f: TextIOBase, /) -> None:
        pass


class FileLogStorage:
    """A [log storage][duron.log.LogStorage] that uses a file to store log entries."""

    __slots__ = ("_lease", "_lock", "_log_file")

    def __init__(self, log_file: str | Path) -> None:
        self._log_file = Path(log_file)
        self._log_file.parent.mkdir(parents=True, exist_ok=True)
        self._lease: TextIOBase | None = None
        self._lock = asyncio.Lock()

    async def stream(self) -> AsyncGenerator[tuple[int, BaseEntry], None]:
        if not self._log_file.exists():
            return

        with Path(self._log_file).open("rb") as f:  # noqa: ASYNC230
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
                                cast("BaseEntry", cast("object", entry)),
                            )
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        pass
                else:
                    # Reached end of file
                    break

    async def acquire_lease(self) -> bytes:
        async with self._lock:
            self._lease = Path(self._log_file).open("a", encoding="utf-8")  # noqa: ASYNC230, SIM115
            lock_file(self._lease)
            return self._lease.fileno().to_bytes(8, "big")

    async def release_lease(self, token: bytes) -> None:
        async with self._lock:
            if self._lease and token == self._lease.fileno().to_bytes(8, "big"):
                unlock_file(self._lease)
                self._lease.close()
                self._lease = None

    async def append(self, token: bytes, entry: Entry) -> int:
        async with self._lock:
            if not self._lease or token != self._lease.fileno().to_bytes(8, "big"):
                msg = "Invalid lease token"
                raise ValueError(msg)

            f = self._lease
            offset = f.tell()
            json.dump(entry, f, separators=(",", ":"))
            _ = f.write("\n")
            f.flush()
            return offset


class MemoryLogStorage:
    """A [log storage][duron.log.LogStorage] that keeps log entries in memory."""

    __slots__ = ("_condition", "_entries", "_leases", "_lock")

    _entries: list[BaseEntry]
    _leases: bytes | None
    _lock: asyncio.Lock
    _condition: asyncio.Condition

    def __init__(self, entries: list[BaseEntry] | None = None) -> None:
        self._entries = entries or []
        self._leases = None
        self._lock = asyncio.Lock()
        self._condition = asyncio.Condition(self._lock)

    async def stream(self) -> AsyncGenerator[tuple[int, BaseEntry], None]:
        # Yield existing entries
        async with self._lock:
            entries_snapshot = self._entries.copy()

        for index in range(len(entries_snapshot)):
            yield (index, entries_snapshot[index])

    async def acquire_lease(self) -> bytes:
        lease_id = uuid.uuid4().bytes
        async with self._lock:
            self._leases = lease_id
        return lease_id

    async def release_lease(self, token: bytes) -> None:
        async with self._lock:
            if token == self._leases:
                self._leases = None

    async def append(self, token: bytes, entry: Entry) -> int:
        async with self._condition:
            if token != self._leases:
                msg = "Invalid lease token"
                raise ValueError(msg)

            offset = len(self._entries)
            self._entries.append(cast("BaseEntry", cast("object", entry)))
            self._condition.notify_all()
            return offset

    async def entries(self) -> list[BaseEntry]:
        async with self._lock:
            return self._entries.copy()
