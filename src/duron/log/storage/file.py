from __future__ import annotations

import asyncio
import json
import threading
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, cast

from typing_extensions import override

from .base import BaseLogStorage, Lease, Offset

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from ..entry import Entry


class FileLogStorage(BaseLogStorage):
    _log_file: Path
    _leases: Lease | None
    _lock: threading.Lock

    def __init__(self, log_file: str | Path):
        self._log_file = Path(log_file)
        self._log_file.parent.mkdir(parents=True, exist_ok=True)
        self._leases = None
        self._lock = threading.Lock()

    @override
    async def stream(
        self, start: Offset | None, live: bool
    ) -> AsyncGenerator[tuple[Offset, Entry], None]:
        if not self._log_file.exists():
            return

        start_offset: int = int.from_bytes(start, "little") if start is not None else 0

        with open(self._log_file, "rb") as f:
            # Seek to start offset
            f.seek(start_offset)

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
                                cast("Entry", cast("object", entry)),
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
                                    cast("Entry", cast("object", entry)),
                                )
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            pass
                    else:
                        await asyncio.sleep(0.1)

    @override
    async def acquire_lease(self) -> Lease:
        lease_id = Lease(uuid.uuid4().bytes)
        with self._lock:
            self._leases = lease_id
        return lease_id

    @override
    async def release_lease(self, token: Lease) -> None:
        with self._lock:
            if token == self._leases:
                self._leases = None

    @override
    async def append(self, token: Lease, entry: Entry) -> Offset:
        with self._lock:
            if token != self._leases:
                raise ValueError("Invalid lease token")

            with open(self._log_file, "a") as f:
                entry_offset = f.tell()
                json.dump(entry, f, separators=(",", ":"))
                _ = f.write("\n")

            return Offset(entry_offset.to_bytes(8, "little"))
