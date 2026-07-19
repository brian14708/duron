from __future__ import annotations

import asyncio
import contextvars
import time
from dataclasses import dataclass
from random import Random
from typing import TYPE_CHECKING, Any, final
from typing_extensions import TypedDict, TypeVar

from duron._hints import JSONValue
from duron._history.encoding import iter_entries
from duron._runtime.ops import (
    Barrier,
    EffectCall,
    FnCall,
    OwnedChannelCreate,
    StreamCreate,
    StreamEmit,
    WaitCall,
    WaitPending,
    create_op,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from duron._execution.invocation import EffectRegistry
    from duron._execution.models import EffectOperation, HandleReference
    from duron._hints import TypeHint
    from duron._runtime.loop import EventLoop
    from duron.storage import EventStore

    _T = TypeVar("_T")


class InputBatch(TypedDict):
    offset: int
    closed: bool
    values: list[JSONValue]


@dataclass(frozen=True, slots=True)
class InputCandidate:
    name: str
    port: str = ""
    offset: int = -1
    buffered: bool = False
    closed: bool = False


class InputSelection(InputBatch):
    name: str


async def read_inputs(
    store: EventStore, run_id: str, port: str, after: int, *, limit: int = 1024
) -> list[tuple[int, JSONValue, bool]]:
    values: list[tuple[int, JSONValue, bool]] = []
    async for offset, entry in iter_entries(
        store, run_id, after=after, stream_id="__input__:" + port, page_size=limit
    ):
        if entry["type"] != "input.accept" or entry["port"] != port:
            msg = "input history routing mismatch"
            raise ValueError(msg)
        values.append((offset, entry.get("value"), entry["closed"]))
        if len(values) >= limit:
            break
    return values


@final
class Context:
    __slots__ = ("_loop", "_run_id", "_seed", "_store", "executor")

    def __init__(
        self,
        loop: EventLoop,
        seed: str,
        *,
        executor: EffectRegistry,
        store: EventStore,
        run_id: str,
    ) -> None:
        self._loop: EventLoop = loop
        self._seed: str = seed
        self.executor = executor
        self._store = store
        self._run_id = run_id

    async def read_input(self, port: str, after: int, *, block: bool) -> InputBatch:
        async def read() -> InputBatch | WaitPending:
            batch = await read_inputs(self._store, self._run_id, port, after)
            if batch or not block:
                return {
                    "offset": batch[-1][0] if batch else after,
                    "closed": bool(batch and batch[-1][2]),
                    "values": [payload for _, payload, closed in batch if not closed],
                }
            return WaitPending.PENDING

        # Record the batch as a runtime call so replay consumes exactly the
        # same messages, even when new deliveries arrive during recovery.
        return await self._wait(read, return_type=InputBatch)

    async def select_input(
        self,
        candidates: list[InputCandidate],
        deadline: float | None,
        completed: Callable[[], set[str]],
    ) -> InputSelection:
        """Journal one observation; only the chosen input's cursor advances.

        Returns:
            The chosen name, input position, closure state, and encoded values.

        """

        async def choose() -> InputSelection | WaitPending:
            for candidate in candidates:
                if not candidate.port:
                    if candidate.name in completed():
                        return {
                            "name": candidate.name,
                            "offset": -1,
                            "closed": False,
                            "values": [],
                        }
                    continue
                if candidate.buffered or candidate.closed:
                    return {
                        "name": candidate.name,
                        "offset": candidate.offset,
                        "closed": candidate.closed,
                        "values": [],
                    }
                batch = await read_inputs(
                    self._store, self._run_id, candidate.port, candidate.offset, limit=1
                )
                if batch:
                    offset, payload, ended = batch[0]
                    return {
                        "name": candidate.name,
                        "offset": offset,
                        "closed": ended,
                        "values": [payload] if not ended else [],
                    }
            if deadline is not None and time.time() >= deadline:
                return {"name": "timeout", "offset": -1, "closed": False, "values": []}
            return WaitPending.PENDING

        return await self._wait(
            choose,
            return_type=InputSelection,
            deadline=deadline,
            suspendable=all(candidate.port for candidate in candidates),
        )

    async def sleep_until(self, deadline: float) -> None:
        async def poll() -> WaitPending | None:  # ruff: ignore[unused-async]  # Uniform durable poll contract.
            return WaitPending.PENDING if time.time() < deadline else None

        await self._wait(poll, return_type=type(None), deadline=deadline)

    async def _wait(
        self,
        poll: Callable[[], Coroutine[Any, Any, _T | WaitPending]],
        *,
        return_type: TypeHint[_T],
        deadline: float | None = None,
        suspendable: bool = True,
    ) -> _T:
        self._check()
        return await create_op(
            self._loop,
            WaitCall(
                poll, return_type, contextvars.copy_context(), deadline, suspendable
            ),
        )

    def _check(self) -> None:
        if asyncio.get_running_loop() is not self._loop:
            msg = "Context can only be used in its context loop"
            raise RuntimeError(msg)

    @property
    def loop(self) -> EventLoop:
        self._check()
        return self._loop

    async def open_owned_channel(
        self, reference: HandleReference, dtype: TypeHint[_T], name: str | None
    ) -> None:
        self._check()
        _ = await create_op(self._loop, OwnedChannelCreate(reference, dtype, name))

    @property
    def seed(self) -> str:
        """The root promise ID, used to seed operation IDs."""
        return self._seed

    @property
    def operation_id(self) -> str:
        """Deterministic id for the current operation position (non-consuming).

        Stable across replay of the same position; falls back to the run seed
        outside a durable task context.
        """
        op_id = self._loop.peek_op_id()
        return self._seed if op_id is None else op_id

    async def run(self, fn: Callable[[], _T], /, *, return_type: TypeHint[_T]) -> _T:
        """Record an internal observation with an explicit result type.

        Returns:
            The result of the function call.

        """
        self._check()
        return await create_op(
            self._loop,
            FnCall(
                callable=fn, return_type=return_type, context=contextvars.copy_context()
            ),
        )

    async def run_effect(
        self, operation: EffectOperation, return_type: TypeHint[_T]
    ) -> _T:
        """Schedule a registered effect with snapshotted caller inputs.

        Returns:
            The effect result.

        """
        self._check()
        return await create_op(
            self._loop, EffectCall(operation, return_type, contextvars.copy_context())
        )

    async def create_output(self, dtype: TypeHint[object], name: str) -> str:
        self._check()
        return await create_op(self._loop, StreamCreate(name=name, dtype=dtype))

    async def emit_output(self, stream_id: str, value: object) -> None:
        self._check()
        await create_op(self._loop, StreamEmit(stream_id, value))

    async def time_us(self) -> int:
        """Get the current deterministic time in microseconds.

        Same clock as :meth:`time`, in the log's native unit, so a consumer that
        needs a log timestamp need not convert back and forth through seconds.

        Returns:
            The current time as an integer number of microseconds.

        """
        self._check()
        _log_offset, time_us = await create_op(self._loop, Barrier())
        return time_us

    async def time(self) -> float:
        """Get the current deterministic time in seconds.

        This provides a deterministic timestamp that is consistent during replay.
        Use this instead of `time.time()` to ensure deterministic behavior.

        Returns:
            The current time in seconds as a float.

        """
        return (await self.time_us()) * 1e-6

    def random(self) -> Random:
        """Get a deterministic random number generator.

        This provides a seeded Random instance that produces consistent results
        during replay. Use this instead of the `random` module to ensure
        deterministic behavior.

        Returns:
            A Random instance seeded with a deterministic operation ID.

        """
        self._check()
        return Random(self._loop.generate_op_id() + self._seed)  # ruff: ignore[suspicious-non-cryptographic-random-usage]
