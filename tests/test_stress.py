"""Stress tests for the durable/replay model.

Two complementary harnesses attack crash-recovery correctness:

* **Prefix truncation** — run a workflow to completion, then for every
  possible crash point (a prefix of the recorded log) seed fresh storage with
  that prefix and resume. This models a crash after exactly ``k`` durable
  entries, deterministically and exhaustively.

* **Chaos cancellation** — cancel ``duron.run(...)`` at random append counts
  (possibly several crashes in a row), then resume to completion.

Both assert the core invariants:

1. The final result equals the uninterrupted reference result.
2. Exactly-once replay: an effect whose completion was durable before the
   crash never re-executes.
3. Output ports deliver the same values (deduped by durable offset).
4. A third run over the finished log re-executes nothing.
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import AsyncIterator, Callable, Coroutine
from typing import TYPE_CHECKING, Any, TypeVar, cast
from typing_extensions import override

import duron
from duron.contrib.storage import FileStorage, MemoryStorage
from duron.log._helper import validate_entry

if TYPE_CHECKING:
    from pathlib import Path

    from duron.log import Entry
    from duron.log._entry import BaseEntry

T = TypeVar("T")

# --- Shared ports (unique names; storages are per-test) ---------------------

numbers_in = duron.Input[int]("stress_numbers")
events_out = duron.Output[str]("stress_events")
cancel_sig = duron.Signal[str]("stress_cancel")
approval_req = duron.Request[str, bool]("stress_approval")

# --- Tracked effects ---------------------------------------------------------

# Executions of effects, as idempotency keys, for the *current* run attempt.
# Tests reset this list before each attempt.
EXECUTIONS: list[str] = []


@duron.effect
async def fx_double(key: str, x: int) -> int:
    EXECUTIONS.append(key)
    # A small real latency stretches the run so chaos cancellation lands
    # mid-execution instead of after completion.
    await asyncio.sleep(0.001)
    return x * 2


@duron.effect(raises=(ValueError,))
async def fx_fail(key: str) -> int:
    EXECUTIONS.append(key)
    await asyncio.sleep(0)
    msg = "declared failure"
    raise ValueError(msg)


@duron.effect
async def fx_slow(key: str) -> str:
    EXECUTIONS.append(key)
    await asyncio.sleep(0.05)
    return "slow done"


@duron.effect
async def fx_count(key: str, n: int) -> AsyncIterator[int]:
    EXECUTIONS.append(key)
    for i in range(n):
        await asyncio.sleep(0)
        yield i


# --- Workflow definitions ----------------------------------------------------


@duron.workflow
async def wf_sequential(
    ctx: duron.WorkflowContext, *, with_roll: bool = True
) -> list[object]:
    # Sequential effects, a declared failure, randomness, sleep, and outputs.
    a = await ctx.call(fx_double, ctx.idempotency_key, 3)
    b = await ctx.call(fx_double, ctx.idempotency_key, a)
    try:
        _ = await ctx.call(fx_fail, ctx.idempotency_key)
    except ValueError:
        b += 1
    roll = ctx.random().randint(1, 10**9)
    await ctx.sleep(0)
    await ctx.emit(events_out, f"done:{b}")
    return [b, roll] if with_roll else [b]


@duron.workflow
async def wf_concurrent(ctx: duron.WorkflowContext) -> list[int]:
    # Concurrent branches racing on the durable loop.

    async def branch(i: int) -> int:
        first = await ctx.call(fx_double, ctx.idempotency_key, i)
        return await ctx.call(fx_double, ctx.idempotency_key, first)

    return list(await asyncio.gather(*(branch(i) for i in range(5))))


@duron.workflow
async def wf_streaming(ctx: duron.WorkflowContext) -> tuple[int, list[int]]:
    # A streaming effect fully consumed, plus its item count.
    async with ctx.stream(fx_count, ctx.idempotency_key, 5) as call:
        items = [item async for item in call]
        count = await call.result()
    return (count, items)


@duron.workflow
async def wf_streaming_twice(ctx: duron.WorkflowContext) -> list[list[int]]:
    # Two sequential streams of the same effect (stream-name reuse).
    out: list[list[int]] = []
    for n in (2, 3):
        async with ctx.stream(fx_count, ctx.idempotency_key, n) as call:
            out.append([item async for item in call])
    return out


@duron.workflow
async def wf_stream_early_exit(ctx: duron.WorkflowContext) -> list[int]:
    # Abandon a streaming effect mid-iteration, then keep working.
    seen: list[int] = []
    async with ctx.stream(fx_count, ctx.idempotency_key, 5) as call:
        async for item in call:
            seen.append(item)
            if item == 1:
                break
    # Positional op ids must stay stable after the abandoned stream.
    seen.append(await ctx.call(fx_double, ctx.idempotency_key, 7))
    return seen


@duron.workflow
async def wf_requests(ctx: duron.WorkflowContext) -> list[bool]:
    # Sequential durable requests answered by the host.
    return [await ctx.request(approval_req, q) for q in ("q1", "q2", "q3")]


@duron.workflow
async def wf_input(ctx: duron.WorkflowContext) -> list[int]:
    # Consume an input port until end-of-input.
    async def next_or_done() -> tuple[bool, int]:
        try:
            return (True, await ctx.receive(numbers_in))
        except duron.PortClosedError:
            return (False, 0)

    seen: list[int] = []
    while True:
        ok, value = await next_or_done()
        if not ok:
            return seen
        seen.append(value)


@duron.workflow
async def wf_input_batches(ctx: duron.WorkflowContext) -> list[list[int]]:
    # Consume an input port in batches via receive_many.
    batches: list[list[int]] = []
    while True:
        batch = await ctx.receive_many(numbers_in, max_items=2, wait=False)
        if not batch:
            return batches
        batches.append(batch)


class WorkflowFailedError(Exception):
    pass


@duron.workflow
async def wf_failing(ctx: duron.WorkflowContext) -> int:
    # Fail deterministically after durable steps; the error must replay.
    _ = await ctx.call(fx_double, ctx.idempotency_key, 2)
    await ctx.emit(events_out, "before-failure")
    msg = "boom"
    raise WorkflowFailedError(msg)


@duron.workflow
async def wf_signal(ctx: duron.WorkflowContext) -> str:
    # A signal interrupting a slow effect, deterministically (signal first).
    try:
        async with ctx.interruptible(cancel_sig):
            return await ctx.call(fx_slow, ctx.idempotency_key)
    except duron.Interrupted as exc:
        return f"interrupted:{exc.value}"


@duron.workflow
async def wf_kitchen_sink(
    ctx: duron.WorkflowContext, *, with_roll: bool = True
) -> dict[str, object]:
    # Everything at once: effects, gather, stream, request, input, signal.
    await ctx.emit(events_out, "start")
    a = await ctx.call(fx_double, ctx.idempotency_key, 1)

    async def branch(i: int) -> int:
        return await ctx.call(fx_double, ctx.idempotency_key, i)

    gathered = await asyncio.gather(branch(a), branch(a + 1))
    async with ctx.stream(fx_count, ctx.idempotency_key, 3) as call:
        streamed = [item async for item in call]
    approved = await ctx.request(approval_req, "ok?")
    first_input = await ctx.receive(numbers_in)
    await ctx.sleep(0)
    roll = ctx.random().randint(0, 10**9)
    await ctx.emit(events_out, "end")
    result: dict[str, object] = {
        "gathered": list(gathered),
        "streamed": streamed,
        "approved": approved,
        "input": first_input,
    }
    if with_roll:
        result["roll"] = roll
    return result


# --- Wiring helpers ----------------------------------------------------------


async def _aiter(*items: T) -> AsyncIterator[T]:
    for item in items:
        await asyncio.sleep(0)
        yield item


async def _cancel_soon() -> AsyncIterator[str]:
    await asyncio.sleep(0.001)
    yield "halt"


class OutCollector:
    """An output sink that records (offset, value) pairs across attempts."""

    def __init__(self) -> None:
        self.items: list[tuple[int, Any]] = []

    async def __call__(self, entries: AsyncIterator[tuple[int, Any]]) -> None:
        async for offset, value in entries:
            self.items.append((offset, value))

    def deduped(self) -> list[tuple[int, Any]]:
        seen: dict[int, Any] = {}
        for offset, value in self.items:
            assert offset not in seen or seen[offset] == value, (
                f"conflicting values at offset {offset}: {seen[offset]!r} vs {value!r}"
            )
            seen[offset] = value
        return sorted(seen.items())

    def values(self) -> list[Any]:
        return [value for _offset, value in self.deduped()]


def _serve_true(_question: str) -> bool:
    return True


def _wire(
    inv: duron.Invocation[Any],
    *,
    out: OutCollector | None = None,
    feed_input: bool = False,
    feed_signal: bool = False,
    serve: bool = False,
) -> duron.Invocation[Any]:
    if out is not None:
        inv = inv.output(events_out, out)
    if feed_input:
        inv = inv.feed(numbers_in, _aiter(10, 20, 30))
    if feed_signal:
        inv = inv.feed(cancel_sig, _cancel_soon())
    if serve:
        inv = inv.serve(approval_req, _serve_true)
    return inv


# --- Invariants --------------------------------------------------------------


def _op_id(key: str) -> str:
    # Strip the run seed from an idempotency key, leaving the op id.
    return key.split(":", 1)[1]


def _completed_op_ids(entries: list[Entry]) -> set[str]:
    return {
        entry["promise_id"] for entry in entries if entry["type"] == "promise.complete"
    }


def _seed(entries: list[Entry]) -> MemoryStorage:
    copies = [cast("BaseEntry", dict(entry)) for entry in entries]
    return MemoryStorage(entries=copies)


async def _run_attempt(
    invocation: duron.Invocation[Any], storage: MemoryStorage, timeout: float = 15.0
) -> object:
    return await asyncio.wait_for(duron.run(invocation, storage), timeout)


async def _run_outcome(
    invocation: duron.Invocation[Any], storage: MemoryStorage
) -> tuple[str, Any]:
    # Run to a comparable outcome: ("ok", value) or ("err", (type, message)).
    try:
        return ("ok", await _run_attempt(invocation, storage))
    except Exception as exc:  # noqa: BLE001
        return ("err", (type(exc).__name__, str(exc)))


# A factory produces a fresh, wired invocation per attempt (invocations are
# single-use).
InvocationFactory = Callable[[OutCollector | None], duron.Invocation[Any]]


async def _assert_crash_at_every_prefix(
    factory: InvocationFactory,
    *,
    with_outputs: bool,
    max_prefixes: int | None = None,
    seed_storage: Callable[[list[Entry]], Coroutine[Any, Any, MemoryStorage]]
    | None = None,
) -> None:
    if seed_storage is None:

        async def memory_seed(entries: list[Entry]) -> MemoryStorage:
            return _seed(entries)

        seed_storage = memory_seed

    # --- reference run -------------------------------------------------
    EXECUTIONS.clear()
    ref_out = OutCollector() if with_outputs else None
    storage = MemoryStorage()
    reference_outcome = await _run_outcome(factory(ref_out), storage)
    reference_entries = [
        validate_entry(raw, i) for i, raw in enumerate(await storage.entries())
    ]
    assert reference_entries, "reference run produced an empty log"

    # --- resume from every crash point --------------------------------
    # Crash points start once the header (init completion) is durable: before
    # that, nothing is recorded and a resume legitimately re-initializes with
    # a fresh nonce, so results may differ from the reference by design.
    first_complete = next(
        i
        for i, entry in enumerate(reference_entries)
        if entry["type"] == "promise.complete"
    )
    points = range(first_complete + 1, len(reference_entries) + 1)
    if max_prefixes is not None:
        step = max(1, len(reference_entries) // max_prefixes)
        points = range(first_complete + 1, len(reference_entries) + 1, step)
    for k in points:
        prefix = reference_entries[:k]
        completed = _completed_op_ids(prefix)
        resume_storage = await seed_storage(prefix)
        EXECUTIONS.clear()
        out = OutCollector() if with_outputs else None
        outcome = await _run_outcome(factory(out), resume_storage)
        assert outcome == reference_outcome, (
            f"crash at {k}: outcome {outcome!r} != reference {reference_outcome!r}"
        )
        # Exactly-once: nothing completed durably before the crash re-executed.
        re_executed = [key for key in EXECUTIONS if _op_id(key) in completed]
        assert not re_executed, (
            f"crash at {k}: effects with durable completions re-executed: {re_executed}"
        )
        if with_outputs:
            # Offsets are backend-specific (entry index vs byte offset), so
            # only values are comparable across backends; within one backend
            # the collector already asserted per-offset consistency.
            assert out is not None
            assert ref_out is not None
            assert out.values() == ref_out.values(), (
                f"crash at {k}: outputs diverged: {out.values()} vs {ref_out.values()}"
            )

        # --- third run: a finished log replays with zero effect executions --
        EXECUTIONS.clear()
        third = await _run_outcome(factory(None), resume_storage)
        assert third == reference_outcome, (
            f"crash at {k}: third run outcome {third!r} != reference"
        )
        assert not EXECUTIONS, (
            f"crash at {k}: completed run re-executed effects: {EXECUTIONS}"
        )


# --- Prefix-truncation tests -------------------------------------------------


async def test_stress_sequential_every_crash_point() -> None:
    def factory(_out: OutCollector | None) -> duron.Invocation[Any]:
        return _wire(wf_sequential(), out=_out)

    await _assert_crash_at_every_prefix(factory, with_outputs=True)


async def test_stress_concurrent_every_crash_point() -> None:
    def factory(_out: OutCollector | None) -> duron.Invocation[Any]:
        return wf_concurrent()

    await _assert_crash_at_every_prefix(factory, with_outputs=False)


async def test_stress_streaming_every_crash_point() -> None:
    def factory(_out: OutCollector | None) -> duron.Invocation[Any]:
        return wf_streaming()

    await _assert_crash_at_every_prefix(factory, with_outputs=False)


async def test_stress_streaming_twice_every_crash_point() -> None:
    def factory(_out: OutCollector | None) -> duron.Invocation[Any]:
        return wf_streaming_twice()

    await _assert_crash_at_every_prefix(factory, with_outputs=False)


async def test_stress_stream_early_exit_every_crash_point() -> None:
    def factory(_out: OutCollector | None) -> duron.Invocation[Any]:
        return wf_stream_early_exit()

    await _assert_crash_at_every_prefix(factory, with_outputs=False)


async def test_stress_failing_workflow_every_crash_point() -> None:
    def factory(_out: OutCollector | None) -> duron.Invocation[Any]:
        return _wire(wf_failing(), out=_out)

    await _assert_crash_at_every_prefix(factory, with_outputs=True)


async def test_stress_requests_every_crash_point() -> None:
    def factory(_out: OutCollector | None) -> duron.Invocation[Any]:
        return _wire(wf_requests(), serve=True)

    await _assert_crash_at_every_prefix(factory, with_outputs=False)


async def test_stress_input_every_crash_point() -> None:
    def factory(_out: OutCollector | None) -> duron.Invocation[Any]:
        return _wire(wf_input(), feed_input=True)

    await _assert_crash_at_every_prefix(factory, with_outputs=False)


async def test_stress_input_batches_every_crash_point() -> None:
    def factory(_out: OutCollector | None) -> duron.Invocation[Any]:
        return _wire(wf_input_batches(), feed_input=True)

    await _assert_crash_at_every_prefix(factory, with_outputs=False)


async def test_stress_signal_every_crash_point() -> None:
    def factory(_out: OutCollector | None) -> duron.Invocation[Any]:
        return _wire(wf_signal(), feed_signal=True)

    await _assert_crash_at_every_prefix(factory, with_outputs=False)


async def test_stress_kitchen_sink_every_crash_point() -> None:
    def factory(_out: OutCollector | None) -> duron.Invocation[Any]:
        return _wire(wf_kitchen_sink(), out=_out, feed_input=True, serve=True)

    await _assert_crash_at_every_prefix(factory, with_outputs=True)


async def test_stress_file_storage_every_crash_point(tmp_path: Path) -> None:
    """Same crash-point sweep, but resumes through the JSON-lines backend."""
    counter = 0

    async def seed_file(entries: list[Entry]) -> MemoryStorage:
        nonlocal counter
        counter += 1
        storage = FileStorage(tmp_path / f"run-{counter}.log")
        lease = await storage.acquire_lease()
        for entry in entries:
            _ = await storage.append(lease, cast("Entry", dict(entry)))
        await storage.release_lease(lease)
        return storage  # type: ignore[return-value]

    def factory(_out: OutCollector | None) -> duron.Invocation[Any]:
        return _wire(wf_sequential(), out=_out)

    await _assert_crash_at_every_prefix(
        factory, with_outputs=True, seed_storage=seed_file
    )


# --- Chaos cancellation fuzzing ----------------------------------------------


class ChaosStorage(MemoryStorage):
    """MemoryStorage that signals when an append budget is exhausted."""

    def __init__(self) -> None:
        super().__init__()
        self.appends = 0
        self.crash_at = -1
        self.crashed = asyncio.Event()

    @override
    async def append(self, token: bytes, entry: Entry) -> int:
        offset = await super().append(token, entry)
        self.appends += 1
        if 0 < self.crash_at <= self.appends and not self.crashed.is_set():
            self.crashed.set()
        return offset


async def _fuzz(
    factory: InvocationFactory,
    *,
    with_outputs: bool,
    iterations: int,
    max_crashes: int,
    seed: int,
) -> None:
    # Reference result for comparison.
    ref_out = OutCollector() if with_outputs else None
    ref_result = await _run_attempt(factory(ref_out), MemoryStorage())

    rng = random.Random(seed)
    for iteration in range(iterations):
        storage = ChaosStorage()
        crashes = rng.randint(1, max_crashes)
        out = OutCollector() if with_outputs else None
        result: Any = None
        for attempt in range(crashes + 1):
            # Crash after a random number of *new* appends, except on the
            # final attempt which runs to completion. The crash must leave
            # the header (init completion, append 2) durable, otherwise the
            # next attempt legitimately re-initializes with a fresh nonce.
            is_last = attempt == crashes
            storage.crash_at = (
                -1 if is_last else max(storage.appends + rng.randint(1, 8), 3)
            )
            storage.crashed.clear()
            task = asyncio.ensure_future(duron.run(factory(out), storage))
            watcher: asyncio.Task[None] | None = None
            if not is_last:

                async def cancel_on_crash(
                    storage: ChaosStorage = storage, task: asyncio.Task[object] = task
                ) -> None:
                    await storage.crashed.wait()
                    # Cancel immediately: any sleep here lets a fast workflow
                    # finish first, and the cancel would no-op.
                    task.cancel()

                watcher = asyncio.ensure_future(cancel_on_crash())
            try:
                result = await asyncio.wait_for(asyncio.shield(task), 15.0)
            except asyncio.CancelledError:
                assert not is_last, "final attempt was cancelled"
                continue
            finally:
                if watcher is not None:
                    watcher.cancel()
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            break
        assert result == ref_result, (
            f"fuzz iteration {iteration} (seed {seed}): result {result!r} "
            f"!= reference {ref_result!r} after {crashes} crashes"
        )
        if with_outputs:
            assert out is not None
            assert ref_out is not None
            assert out.values() == ref_out.values(), (
                f"fuzz iteration {iteration}: outputs diverged"
            )


async def test_fuzz_sequential() -> None:
    def factory(_out: OutCollector | None) -> duron.Invocation[Any]:
        # Roll excluded: each fuzz iteration runs under its own nonce, so
        # nonce-derived values legitimately differ from the reference run.
        return _wire(wf_sequential(with_roll=False), out=_out)

    await _fuzz(factory, with_outputs=True, iterations=20, max_crashes=3, seed=11)


async def test_fuzz_concurrent() -> None:
    def factory(_out: OutCollector | None) -> duron.Invocation[Any]:
        return wf_concurrent()

    await _fuzz(factory, with_outputs=False, iterations=20, max_crashes=3, seed=22)


async def test_fuzz_streaming() -> None:
    def factory(_out: OutCollector | None) -> duron.Invocation[Any]:
        return wf_streaming()

    await _fuzz(factory, with_outputs=False, iterations=20, max_crashes=3, seed=33)


async def test_fuzz_streaming_twice() -> None:
    def factory(_out: OutCollector | None) -> duron.Invocation[Any]:
        return wf_streaming_twice()

    await _fuzz(factory, with_outputs=False, iterations=20, max_crashes=3, seed=99)


async def test_fuzz_stream_early_exit() -> None:
    def factory(_out: OutCollector | None) -> duron.Invocation[Any]:
        return wf_stream_early_exit()

    await _fuzz(factory, with_outputs=False, iterations=20, max_crashes=3, seed=111)


async def test_fuzz_input_batches() -> None:
    def factory(_out: OutCollector | None) -> duron.Invocation[Any]:
        return _wire(wf_input_batches(), feed_input=True)

    await _fuzz(factory, with_outputs=False, iterations=20, max_crashes=2, seed=122)


async def test_fuzz_requests() -> None:
    def factory(_out: OutCollector | None) -> duron.Invocation[Any]:
        return _wire(wf_requests(), serve=True)

    await _fuzz(factory, with_outputs=False, iterations=20, max_crashes=3, seed=44)


async def test_fuzz_input() -> None:
    def factory(_out: OutCollector | None) -> duron.Invocation[Any]:
        return _wire(wf_input(), feed_input=True)

    await _fuzz(factory, with_outputs=False, iterations=20, max_crashes=2, seed=55)


async def test_fuzz_signal() -> None:
    def factory(_out: OutCollector | None) -> duron.Invocation[Any]:
        return _wire(wf_signal(), feed_signal=True)

    await _fuzz(factory, with_outputs=False, iterations=20, max_crashes=2, seed=66)


async def test_fuzz_kitchen_sink() -> None:
    def factory(_out: OutCollector | None) -> duron.Invocation[Any]:
        return _wire(
            wf_kitchen_sink(with_roll=False), out=_out, feed_input=True, serve=True
        )

    await _fuzz(factory, with_outputs=True, iterations=20, max_crashes=3, seed=77)


@duron.workflow
async def wf_many_steps(ctx: duron.WorkflowContext, n: int) -> int:
    # A long sequential chain, producing a large log for slow replays.
    total = 0
    for _ in range(n):
        total += await ctx.call(fx_double, ctx.idempotency_key, 1)
    return total


async def test_stress_cancel_during_replay() -> None:
    """Cancel mid-`_resume` (while a long log is replaying), then finish."""
    storage = MemoryStorage()
    reference_result = await _run_attempt(wf_many_steps(60), storage)
    reference_entries = [
        validate_entry(raw, i) for i, raw in enumerate(await storage.entries())
    ]

    # Leave two steps unexecuted so each resume must replay ~all entries
    # before going live; replay is then slow enough to cancel inside of.
    prefix = reference_entries[:-4]
    rng = random.Random(5)
    live_storage = _seed(prefix)
    for _ in range(8):
        task = asyncio.ensure_future(duron.run(wf_many_steps(60), live_storage))
        await asyncio.sleep(rng.random() * 0.01)
        task.cancel()
        _ = await asyncio.gather(task, return_exceptions=True)

    result = await _run_attempt(wf_many_steps(60), live_storage)
    assert result == reference_result


@duron.workflow
async def wf_signal_in_receive(ctx: duron.WorkflowContext) -> str:
    # Interrupted while blocked on an input that never arrives.
    try:
        async with ctx.interruptible(cancel_sig):
            value = await ctx.receive(numbers_in)
            return f"got:{value}"
    except duron.Interrupted as exc:
        return f"interrupted:{exc.value}"


async def test_stress_signal_in_receive_every_crash_point() -> None:
    def factory(_out: OutCollector | None) -> duron.Invocation[Any]:
        return _wire(wf_signal_in_receive(), feed_signal=True)

    await _assert_crash_at_every_prefix(factory, with_outputs=False)


async def test_fuzz_signal_in_receive() -> None:
    def factory(_out: OutCollector | None) -> duron.Invocation[Any]:
        return _wire(wf_signal_in_receive(), feed_signal=True)

    await _fuzz(factory, with_outputs=False, iterations=20, max_crashes=2, seed=133)
