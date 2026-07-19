from __future__ import annotations

import asyncio
import copy
from collections import deque
from dataclasses import dataclass
from itertools import pairwise
from typing import TYPE_CHECKING

from duron._runtime.storage.helper import encode_entry
from duron.errors import LeaseLostError, StorageError

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from duron._runtime.storage.backend import HistoryRecord, HistoryStore, LeaseToken
    from duron._runtime.storage.entry import Entry


@dataclass(frozen=True)
class BatchLimits:
    """Coalescing targets for one atomic backend append.

    Indivisible groups larger than either target commit alone.
    """

    max_entries: int = 64
    max_bytes: int = 256 * 1024

    def __post_init__(self) -> None:
        for name, value in (
            ("max_entries", self.max_entries),
            ("max_bytes", self.max_bytes),
        ):
            if type(value) is not int or value <= 0:
                msg = f"{name} must be a positive integer"
                raise ValueError(msg)


@dataclass
class _Group:
    entries: tuple[Entry, ...]
    records: tuple[HistoryRecord, ...]
    encoded_bytes: int
    receipt: asyncio.Future[tuple[int, ...]]
    started: bool = False


class AppendWriter:
    """One bounded, ordered writer; receipts and application follow commit."""

    def __init__(
        self,
        backend: HistoryStore,
        run_id: str,
        token: LeaseToken,
        limits: BatchLimits,
        apply: Callable[[int, Entry], object],
        notify: Callable[[], None],
        abort: Callable[[LeaseLostError | StorageError], None],
    ) -> None:
        self._backend = backend
        self._run_id = run_id
        self._token = token
        self._limits = limits
        self._apply = apply
        self._notify = notify
        self._abort = abort
        self._queue: deque[_Group] = deque()
        self._entries = 0
        self._bytes = 0
        self._admission = asyncio.Lock()
        self._space = asyncio.Event()
        self._worker: asyncio.Task[None] | None = None
        self._cancel_requested = False
        self._error: LeaseLostError | StorageError | None = None
        self._closed = False
        self._last_offset: int | None = None

    async def append(self, entries: Sequence[Entry]) -> tuple[int, ...]:
        if not entries:
            return ()
        async with self._admission:
            self.check_open()
            # Only one admission candidate may allocate a snapshot while the
            # queue is full, including when an indivisible group is oversized.
            snapshot = tuple(copy.deepcopy(entry) for entry in entries)
            # Encoding validates each snapshot before it can enter the queue.
            records = tuple(encode_entry(entry) for entry in snapshot)
            receipt: asyncio.Future[tuple[int, ...]] = (
                asyncio.get_running_loop().create_future()
            )
            group = _Group(
                snapshot,
                records,
                sum(len(record.payload) for record in records),
                receipt,
            )
            while self._entries and (
                self._entries + len(snapshot) > self._limits.max_entries
                or self._bytes + group.encoded_bytes > self._limits.max_bytes
            ):
                self.check_open()
                self._space.clear()
                await self._space.wait()
            self.check_open()
            self._queue.append(group)
            self._entries += len(snapshot)
            self._bytes += group.encoded_bytes
            if self._worker is None or self._worker.done():
                self._cancel_requested = False
                self._worker = asyncio.create_task(self._write())
        try:
            return await asyncio.shield(receipt)
        except asyncio.CancelledError:
            if group.started and not receipt.done():
                self._fail(
                    StorageError("log append cancelled with an ambiguous outcome")
                )
                self._cancel_worker()
            elif not group.started and group in self._queue:
                self._queue.remove(group)
                self._release(group)
            receipt.cancel()
            raise

    def check_open(self) -> None:
        """Reject reads of receipts as well as new writes after owner failure.

        Raises:
            StorageError: If the writer has failed or closed.

        """
        if self._error is not None:
            raise self._error
        if self._closed:
            msg = "append writer is closed"
            raise StorageError(msg)

    def _cancel_worker(self) -> None:
        if self._worker is not None and not self._cancel_requested:
            self._cancel_requested = True
            self._worker.cancel()

    def _release(self, group: _Group) -> None:
        self._entries -= len(group.entries)
        self._bytes -= group.encoded_bytes
        self._space.set()

    def _fail(self, error: LeaseLostError | StorageError) -> None:
        if self._error is None:
            self._error = error
            self._abort(error)
        self._space.set()

    @staticmethod
    def _reject(group: _Group, error: BaseException) -> None:
        if not group.receipt.done():
            group.receipt.set_exception(error)
            # A cancelled caller may have stopped observing its receipt.
            _ = group.receipt.exception()

    async def _write(self) -> None:
        active: list[_Group] = []
        try:
            while self._queue:
                # Coalesce the operations already runnable in this host turn.
                await asyncio.sleep(0)
                if not self._queue:
                    break
                self.check_open()
                active = list(self._queue)
                self._queue.clear()
                for group in active:
                    group.started = True
                entries = [entry for group in active for entry in group.entries]
                records = [record for group in active for record in group.records]
                offsets = tuple(
                    await self._backend.append(self._run_id, self._token, records)
                )
                if (
                    len(offsets) != len(entries)
                    or any(type(offset) is not int for offset in offsets)
                    or any(a >= b for a, b in pairwise(offsets))
                    or (
                        self._last_offset is not None
                        and offsets[0] <= self._last_offset
                    )
                ):
                    msg = "backend returned invalid append offsets"
                    raise StorageError(msg)  # ruff: ignore[raise-within-try]
                for offset, entry in zip(offsets, entries, strict=True):
                    self._apply(offset, entry)
                self._last_offset = offsets[-1]
                self._notify()
                start = 0
                for group in active:
                    end = start + len(group.entries)
                    if not group.receipt.done():
                        group.receipt.set_result(offsets[start:end])
                    self._release(group)
                    start = end
                active = []
        except (Exception, asyncio.CancelledError) as exc:  # ruff: ignore[blind-except]
            if isinstance(exc, LeaseLostError):
                error: LeaseLostError | StorageError = LeaseLostError(
                    "backend lease was lost while appending to the run"
                )
            elif isinstance(exc, asyncio.CancelledError):
                error = StorageError("log append cancelled with an ambiguous outcome")
            else:
                error = StorageError(
                    "backend append failed with an ambiguous outcome; abandon this "
                    "session and reconcile through replay"
                )
            error.__cause__ = exc
            if active or not isinstance(exc, asyncio.CancelledError):
                self._fail(error)
            for group in (*active, *self._queue):
                self._reject(group, self._error or error)
                self._release(group)
            self._queue.clear()

    async def close(self) -> None:
        self._closed = True
        self._space.set()
        if self._worker is not None and not self._worker.done():
            self._cancel_worker()
            _ = await asyncio.gather(self._worker, return_exceptions=True)
        while self._queue:
            group = self._queue.popleft()
            self._reject(group, StorageError("session stopped before append"))
            self._release(group)

    async def drain(self) -> None:
        if self._worker is not None:
            await self._worker
        if self._error is not None:
            raise self._error
