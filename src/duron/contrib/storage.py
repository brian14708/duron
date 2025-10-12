from __future__ import annotations

import asyncio
import json
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, cast, final

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from duron.log import AnyEntry, Entry


@final
class FileLogStorage:
    __slots__ = ("_leases", "_lock", "_log_file")

    _log_file: Path
    _leases: bytes | None
    _lock: asyncio.Lock

    def __init__(self, log_file: str | Path) -> None:
        self._log_file = Path(log_file)
        self._log_file.parent.mkdir(parents=True, exist_ok=True)
        self._leases = None
        self._lock = asyncio.Lock()

    async def stream(
        self,
        start: int | None,
        /,
        *,
        live: bool,
    ) -> AsyncGenerator[tuple[int, AnyEntry], None]:
        if not self._log_file.exists():
            return

        start_offset: int = start if start is not None else 0

        with Path(self._log_file).open("rb") as f:  # noqa: ASYNC230
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
        async with self._lock:
            if token != self._leases:
                msg = "Invalid lease token"
                raise ValueError(msg)

            with Path(self._log_file).open("a", encoding="utf-8") as f:  # noqa: ASYNC230
                offset = f.tell()
                json.dump(entry, f, separators=(",", ":"))
                _ = f.write("\n")
                return offset

    async def flush(self, _token: bytes) -> None:
        pass


@final
class MemoryLogStorage:
    __slots__ = ("_condition", "_entries", "_leases", "_lock")

    _entries: list[AnyEntry]
    _leases: bytes | None
    _lock: asyncio.Lock
    _condition: asyncio.Condition

    def __init__(self, entries: list[AnyEntry] | None = None) -> None:
        self._entries = entries or []
        self._leases = None
        self._lock = asyncio.Lock()
        self._condition = asyncio.Condition(self._lock)

    async def stream(
        self,
        start: int | None,
        /,
        *,
        live: bool,
    ) -> AsyncGenerator[tuple[int, AnyEntry], None]:
        start_index: int = start + 1 if start is not None else 0

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
            self._entries.append(cast("AnyEntry", cast("object", entry)))
            self._condition.notify_all()
            return offset

    async def flush(self, token: bytes) -> None:
        pass

    async def entries(self) -> list[AnyEntry]:
        async with self._lock:
            return self._entries.copy()
