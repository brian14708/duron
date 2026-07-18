from __future__ import annotations

from typing import TYPE_CHECKING
from typing_extensions import Protocol

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from duron.log._entry import BaseEntry, Entry


class LogStorage(Protocol):
    """Protocol for persistent storage of operation logs.

    Built-in backends use opaque fencing tokens. Acquiring a new lease invalidates
    older tokens, and append() must reject stale tokens.
    """

    def stream(self) -> AsyncGenerator[tuple[int, BaseEntry], None]:
        """Stream log entries from storage.

        Yields:
            Tuple of (log_index, entry) for each log entry in order.

        Note:
            Log indices are monotonically increasing but may have gaps.

        """
        ...

    async def acquire_lease(self) -> bytes:
        """Acquire a fencing lease for appending to the log.

        Returns:
            Opaque lease token to be used in append() and release_lease() calls.

        Raises:
            Exception: if the backend cannot establish its locking guarantee.

        Note:
            Acquisition does not wait for or fail because of an existing lease.
            Instead, the new token immediately supersedes the previous token.
            Backends must atomically validate the current token while appending.

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
