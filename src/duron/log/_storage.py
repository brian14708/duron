from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from duron.log._entry import BaseEntry, Entry


class LogStorage(Protocol):
    """Protocol for persistent storage of operation logs.

    The lease mechanism ensures exclusive access for appending entries,
    preventing concurrent writes from multiple processes.
    """

    def stream(
        self,
        start: int | None,
        /,
        *,
        live: bool,
    ) -> AsyncGenerator[tuple[int, BaseEntry], None]:
        """Stream log entries from storage.

        Args:
            start: Starting log index (inclusive). If None, stream from beginning.
            live: If True, continue streaming new entries as they are appended.
                  If False, stop after reading existing entries.

        Yields:
            Tuple of (log_index, entry) for each log entry in order.

        Note:
            Log indices are monotonically increasing but may have gaps.
            During replay phase, this is called with live=False to restore state.
            During live execution, this may be called with live=True to monitor logs.
        """
        ...

    async def acquire_lease(self) -> bytes:
        """Acquire an exclusive lease for appending to the log.

        Returns:
            Opaque lease token to be used in append() and release_lease() calls.

        Raises:
            Exception: if lease cannot be acquired (e.g., already held by another
                       process).

        Note:
            Leases provide concurrency control to ensure only one invoke can append
            to a log at a time, preventing interleaved writes from multiple processes.
        """
        ...

    async def release_lease(self, lease: bytes, /) -> None:
        """Release a previously acquired lease.

        Args:
            lease: Lease token returned by acquire_lease().

        Note:
            Should be called when invoke completes or encounters an error.
            Implementations should be idempotent.
        """
        ...

    async def append(self, lease: bytes, entry: Entry, /) -> int:
        """Append a new entry to the log.

        Args:
            lease: Valid lease token from acquire_lease().
            entry: Log entry to append (promise/create, promise/complete, stream/emit,
                   etc).

        Returns:
            Log index of the appended entry.

        Raises:
            Exception: if lease is invalid or expired.

        Note:
            Appends must be atomic and durable. The returned index must be
            monotonically increasing and consistent with stream() output.
        """
        ...
