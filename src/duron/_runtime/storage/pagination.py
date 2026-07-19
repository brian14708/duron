from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from duron._runtime.storage.backend import HistoryStore
    from duron._runtime.storage.entry import Entry

from duron._runtime.storage.helper import decode_record


async def iter_entries(
    backend: HistoryStore,
    run_id: str,
    *,
    after: int | None = None,
    page_size: int = 1024,
    stream_id: str | None = None,
) -> AsyncGenerator[tuple[int, Entry], None]:
    """Read bounded pages until the observed end, including after short pages.

    The consumer owns its last consumed cursor. Closing this iterator discards
    prefetched entries; resuming after that cursor loses no records.

    Yields:
        Committed offsets and entries in increasing order.

    """
    cursor = after
    while page := await backend.read(
        run_id, after=cursor, limit=page_size, stream_id=stream_id
    ):
        for offset, record in page:
            yield offset, decode_record(record, offset)
            cursor = offset
