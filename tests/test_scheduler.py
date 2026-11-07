# pyright: reportPrivateUsage=false
# ruff: file-ignore[private-member-access]

from __future__ import annotations

import asyncio
import sqlite3
from contextlib import closing
from typing import TYPE_CHECKING, assert_type, cast

import pytest

import duron
import duron._runtime.host as run_host
from duron import OutputCursor
from duron._history.encoding import encode_entry
from duron._runtime.history import entries as history_entries
from duron._runtime.history import read_outcome
from duron._runtime.host import SQLiteScheduler
from duron._runtime.machine import RunMachine
from duron.errors import (
    ApplicationError,
    ContractMismatchError,
    FailureKind,
    PortClosedError,
    PostConflictError,
    RunCancelledError,
    RunFailedError,
    RunNotFoundError,
    StorageError,
    SubmissionConflictError,
    WorkflowDefinitionError,
)
from duron.storage import SQLiteStore
from tests.crash_worker import CrashPorts, unfinished_flow
from tests.crash_worker import flow as crash_flow
from tests.helpers import assert_unloaded, crash_process, wait_suspended

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    from duron._history.entries import Entry
    from duron.storage import ClaimToken
    from duron.storage._protocol import Event, EventReader, InboxMessage, ScheduledRun


class Ports(duron.PortSet):
    commands = duron.Input[int]()
    events = duron.Output[int]()


async def test_transient_replay_read_failure_retries_without_error_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = SQLiteStore.read
    failures: list[bool] = []

    async def read(
        self: SQLiteStore,
        run_id: str,
        *,
        after: int | None = None,
        limit: int = 1024,
        stream_id: str | None = None,
    ) -> Sequence[tuple[int, Event]]:
        if (
            run_id == "transient"
            and limit == 1024
            and stream_id is None
            and not failures
        ):
            failures.append(True)
            msg = "transient replay read failure"
            raise StorageError(msg)
        return await original(
            self, run_id, after=after, limit=limit, stream_id=stream_id
        )

    monkeypatch.setattr(SQLiteStore, "read", read)

    @duron.workflow
    async def flow(_ctx: duron.WorkflowContext) -> int:
        return 42

    async with asyncio.timeout(3), duron.SQLiteScheduler(":memory:") as scheduler:
        run = await scheduler.submit(flow, "transient")
        assert await run.result() == 42
        assert (await run.status()).state is duron.RunState.SUCCEEDED
        outcome = await read_outcome(scheduler._store, run.id)
        assert outcome is not None
        assert "error" not in outcome
    assert failures == [True]


async def test_cancel_history_mismatch_does_not_starve_healthy_jobs() -> None:
    @duron.workflow
    async def flow(_ctx: duron.WorkflowContext) -> int:
        return 42

    async with asyncio.timeout(3), duron.SQLiteScheduler(":memory:") as scheduler:
        bad = await scheduler.submit(flow.ref, "a-bad")
        header = encode_entry({
            "id": "different-seed",
            "type": "promise.create",
            "source": "effect",
            "ts": 0,
            "ref": {"name": "other-flow"},
            "args": [],
            "kwargs": {},
        })
        await scheduler._store.create(bad.id, header)
        await bad.cancel()
        dispatcher = scheduler._dispatcher
        good = await scheduler.submit(flow, "z-good")
        assert await good.result() == 42
        # Public observation rejects the mismatched header itself. Check the
        # scheduler disposition as well, to rule out a hidden retry loop.
        with pytest.raises(ContractMismatchError):
            await bad.result()
        while True:
            changed = scheduler.watch(bad.id)
            job = await scheduler._store.job_get("default", bad.id)
            if job.state == "failed":
                break
            await changed()
        assert job.error_kind is FailureKind.HISTORY_MISMATCH
        assert scheduler._dispatcher is dispatcher
        assert await read_outcome(scheduler._store, bad.id) is None
        page = await scheduler._store.read(bad.id)
        assert [event for _, event in page] == [header]


async def test_cancellation_after_candidate_snapshot_prevents_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = asyncio.Event()
    dispatch = asyncio.Event()
    calls: list[bool] = []
    original = SQLiteScheduler._job_candidates

    async def candidates(self: SQLiteScheduler, after: str) -> Sequence[ScheduledRun]:
        jobs = await original(self, after)
        if jobs and not captured.is_set():
            captured.set()
            await dispatch.wait()
        return jobs

    monkeypatch.setattr(SQLiteScheduler, "_job_candidates", candidates)

    @duron.workflow
    async def flow(_ctx: duron.WorkflowContext) -> int:
        calls.append(True)
        return 42

    async with asyncio.timeout(3), duron.SQLiteScheduler(":memory:") as scheduler:
        run = await scheduler.submit(flow, "cancelled")
        await captured.wait()
        await run.cancel()
        dispatch.set()
        with pytest.raises(RunCancelledError):
            await run.result()
    assert calls == []


async def test_cancelled_send_waiter_still_delivers_committed_input(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    drained = asyncio.Event()
    committed = asyncio.Event()
    pending = SQLiteStore.inbox_pending
    post = SQLiteStore.inbox_post

    async def inbox_pending(
        self: SQLiteStore, namespace: str, run_id: str
    ) -> Sequence[InboxMessage]:
        messages = await pending(self, namespace, run_id)
        if not messages:
            drained.set()
        return messages

    async def inbox_post(
        self: SQLiteStore,
        namespace: str,
        inbox_id: str,
        run_id: str,
        kind: str,
        port: str,
        payload: bytes,
    ) -> None:
        await post(self, namespace, inbox_id, run_id, kind, port, payload)
        committed.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(SQLiteStore, "inbox_pending", inbox_pending)
    monkeypatch.setattr(SQLiteStore, "inbox_post", inbox_post)

    @duron.workflow(ports=Ports)
    async def flow(ctx: duron.WorkflowContext) -> int:
        await ctx.port(Ports.events).emit(1)
        return await ctx.port(Ports.commands).receive()

    async with asyncio.timeout(3), duron.SQLiteScheduler(":memory:") as scheduler:
        run = await scheduler.submit(flow, "input")
        assert await anext(run.port(Ports.events).reader()) == 1
        await drained.wait()
        send = asyncio.create_task(run.port(Ports.commands).send(42))
        await committed.wait()
        send.cancel()
        with pytest.raises(asyncio.CancelledError):
            await send
        assert await run.result() == 42


async def test_sqlite_scheduler_blocks_interrupted_effect_on_reopen(
    tmp_path: Path,
) -> None:
    started = asyncio.Event()
    finish = asyncio.Event()

    @duron.effect
    async def wait(_ctx: duron.EffectContext) -> int:
        started.set()
        await finish.wait()
        return 42

    @duron.workflow(ports=Ports)
    async def flow(ctx: duron.WorkflowContext, value: int) -> int:
        return value + await ctx.call(wait)

    async def submit(scheduler: duron.Scheduler) -> duron.Run[int, Ports]:
        run = await scheduler.submit(flow, "recover", 1)
        assert_type(run, duron.Run[int, Ports])
        assert_type(run.port(Ports.commands), duron.Sender[int])
        return run

    scheduler = duron.SQLiteScheduler(tmp_path / "lifecycle.db", workflows=[flow])
    with pytest.raises(RuntimeError, match="not active"):
        await scheduler.submit(flow, "outside", 1)
    async with scheduler:
        with pytest.raises(RuntimeError, match="already active"):
            async with scheduler:
                pytest.fail("overlapping entry succeeded")
        run = await submit(scheduler)
        await asyncio.wait_for(started.wait(), 3)
        await scheduler.shutdown(timeout=0)
    with pytest.raises(RuntimeError, match="not active"):
        await run.status()
    async with scheduler:
        restored = await scheduler.open(flow.ref, "recover")
        with pytest.raises(
            RunFailedError, match="interrupted before its outcome"
        ) as blocked:
            await asyncio.wait_for(restored.result(), 3)
        assert (await restored.status()).state is duron.RunState.BLOCKED
        assert blocked.value.kind is FailureKind.INTERRUPTED_EFFECT
    finish.set()

    transient = duron.SQLiteScheduler(":memory:")
    async with transient:
        assert await (await transient.submit(flow, "transient", 0)).result() == 42
    async with transient:
        with pytest.raises(RunNotFoundError):
            await transient.open(flow, "transient")


async def test_sqlite_scheduler_startup_failure_closes_storage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_wake = SQLiteScheduler._job_wake
    original_close = SQLiteStore.close
    closed: list[SQLiteStore] = []

    async def wake(self: SQLiteScheduler) -> None:
        await original_wake(self)
        msg = "startup failed after opening storage"
        raise RuntimeError(msg)

    async def close(self: SQLiteStore) -> None:
        await original_close(self)
        closed.append(self)

    scheduler = duron.SQLiteScheduler(":memory:")
    monkeypatch.setattr(SQLiteScheduler, "_job_wake", wake)
    monkeypatch.setattr(SQLiteStore, "close", close)
    with pytest.raises(RuntimeError, match="startup failed"):
        async with scheduler:
            pytest.fail("startup succeeded")
    assert len(closed) == 1
    monkeypatch.setattr(SQLiteScheduler, "_job_wake", original_wake)
    async with scheduler:
        pass
    assert len(closed) == 2


async def test_structured_jobs_inbox_and_history(tmp_path: Path) -> None:
    @duron.workflow(ports=Ports)
    async def flow(ctx: duron.WorkflowContext, base: int) -> int:
        await ctx.port(Ports.events).emit(base)
        return base + await ctx.port(Ports.commands).receive()

    path = tmp_path / "structured.db"
    async with duron.SQLiteScheduler(
        path, workflows=[flow], namespace="app", idle_timeout=0.01
    ) as scheduler:
        run = await scheduler.submit(flow, "one", 3)
        assert await anext(run.port(Ports.events).reader()) == 3
        await wait_suspended(run)
        with closing(sqlite3.connect(path)) as connection:
            row = connection.execute(
                "SELECT state, retry_at, workflow_name, "
                "json_extract(inputs, '$.base') FROM jobs "
                "WHERE namespace = 'app' AND run_id = 'one'"
            ).fetchone()
            assert row == ("suspended", None, flow.name, 3)
            assert connection.execute(
                "SELECT DISTINCT namespace, run_id, typeof(data) "
                "FROM log_entries WHERE event_type = 'stream.emit'"
            ).fetchall() == [("app", "one", "text")]
        await run.port(Ports.commands).send(4, idempotency_key="four")
        assert await run.result() == 7
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute(
            "SELECT state FROM jobs WHERE namespace = 'app' AND run_id = 'one'"
        ).fetchall() == [("succeeded",)]


async def test_suspended_runs_keep_deadlines_and_cancel_after_restart(
    tmp_path: Path,
) -> None:
    @duron.workflow(ports=Ports)
    async def timer(ctx: duron.WorkflowContext) -> str:
        selected = await ctx.select(input=ctx.port(Ports.commands), timeout=0.15)
        await ctx.port(Ports.events).emit(1)
        await ctx.sleep(0.15)
        return selected.name

    @duron.workflow(ports=Ports)
    async def waiting(ctx: duron.WorkflowContext) -> int:
        return await ctx.port(Ports.commands).receive()

    path = tmp_path / "suspended.db"
    async with asyncio.timeout(5):
        async with duron.SQLiteScheduler(
            path, workflows=[timer, waiting], idle_timeout=0.01
        ) as scheduler:
            store = scheduler._store
            timed = await scheduler.submit(timer, "timer")
            cancelled = await scheduler.submit(waiting, "cancel")
            await wait_suspended(timed)
            await wait_suspended(cancelled)
            await assert_unloaded(store, "timer")
            await assert_unloaded(store, "cancel")
        await asyncio.sleep(0.03)
        async with duron.SQLiteScheduler(
            path, workflows=[timer, waiting], idle_timeout=0.01
        ) as scheduler:
            store = scheduler._store
            cancelled = await scheduler.open(waiting, "cancel")
            assert (await cancelled.status()).state is duron.RunState.SUSPENDED
            await cancelled.cancel()
            with pytest.raises(RunCancelledError):
                await cancelled.result()
            timed = await scheduler.open(timer, "timer")
            assert await anext(timed.port(Ports.events).reader()) == 1
            assert await timed.result() == "timeout"


async def test_inbox_during_suspension_is_not_lost(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    suspending = asyncio.Event()
    posted = asyncio.Event()
    original_suspend = SQLiteScheduler._job_suspend
    original_post = SQLiteScheduler._inbox_post

    async def suspend(
        self: SQLiteScheduler, run_id: str, deadline: float | None
    ) -> bool:
        suspending.set()
        await posted.wait()
        return await original_suspend(self, run_id, deadline)

    async def post(
        self: SQLiteScheduler,
        inbox_id: str,
        run_id: str,
        kind: str,
        port: str,
        payload: bytes,
    ) -> None:
        await original_post(self, inbox_id, run_id, kind, port, payload)
        posted.set()

    monkeypatch.setattr(SQLiteScheduler, "_job_suspend", suspend)
    monkeypatch.setattr(SQLiteScheduler, "_inbox_post", post)

    @duron.workflow(ports=Ports)
    async def flow(ctx: duron.WorkflowContext) -> int:
        return await ctx.port(Ports.commands).receive()

    async with (
        asyncio.timeout(3),
        duron.SQLiteScheduler(
            tmp_path / "suspend.db", workflows=[flow], idle_timeout=0.01
        ) as scheduler,
    ):
        run = await scheduler.submit(flow, "race")
        await suspending.wait()
        await run.port(Ports.commands).send(7)
        assert await run.result() == 7


async def test_waiting_runs_do_not_block_dispatch_and_recover(tmp_path: Path) -> None:
    @duron.workflow(ports=Ports)
    async def waiting(ctx: duron.WorkflowContext) -> int:
        await ctx.port(Ports.events).emit(1)
        return await ctx.port(Ports.commands).receive()

    @duron.workflow
    async def immediate(_ctx: duron.WorkflowContext) -> int:
        return 42

    path = tmp_path / "concurrent.db"
    async with asyncio.timeout(10):
        async with duron.SQLiteScheduler(
            path, workflows=[waiting, immediate]
        ) as scheduler:
            for run_id in ("first", "second"):
                run = await scheduler.submit(waiting, run_id)
                assert await anext(run.port(Ports.events).reader()) == 1
            result = await scheduler.submit(immediate, "immediate")
            assert await result.result() == 42
        async with duron.SQLiteScheduler(path, workflows=[waiting]) as scheduler:
            for run_id in ("first", "second"):
                run = await scheduler.open(waiting, run_id)
                await run.port(Ports.commands).send(7)
                assert await run.result() == 7
                assert [value async for value in run.port(Ports.events)] == [1]


async def test_shutdown_during_cleanup_releases_run_claim(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    releasing = asyncio.Event()
    finish = asyncio.Event()
    original = SQLiteStore.release
    key = "result"

    async def release(self: SQLiteStore, run_id: str, token: ClaimToken) -> None:
        if run_id == key:
            releasing.set()
            await finish.wait()
        await original(self, run_id, token)

    monkeypatch.setattr(SQLiteStore, "release", release)

    @duron.workflow
    async def flow(_ctx: duron.WorkflowContext) -> int:
        return 42

    path = tmp_path / "release.db"
    store = SQLiteStore(path)
    async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
        run = await scheduler.submit(flow, "result")
        assert await run.result() == 42
        await asyncio.wait_for(releasing.wait(), 3)
        asyncio.get_running_loop().call_soon(finish.set)
    token = await store.claim(key)
    await store.release(key, token)
    await store.close()


async def test_shutdown_during_acquisition_releases_run_claim(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    claimed = asyncio.Event()
    finish = asyncio.Event()
    original = SQLiteStore.claim
    key = "acquiring"

    async def claim(self: SQLiteStore, run_id: str) -> ClaimToken:
        token = await original(self, run_id)
        if run_id == key:
            claimed.set()
            await finish.wait()
        return token

    monkeypatch.setattr(SQLiteStore, "claim", claim)

    @duron.workflow
    async def flow(_ctx: duron.WorkflowContext) -> int:
        return 42

    async with asyncio.timeout(3):
        path = tmp_path / "acquire.db"
        async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
            await scheduler.submit(flow, "acquiring")
            await claimed.wait()
            asyncio.get_running_loop().call_soon(finish.set)
        async with SQLiteStore(path) as store:
            token = await store.claim(key)
            await store.release(key, token)


async def test_exclusive_database_ownership_and_namespace_keys(tmp_path: Path) -> None:
    path = tmp_path / "namespaces.db"
    calls: list[int] = []

    @duron.workflow(ports=Ports)
    async def flow(ctx: duron.WorkflowContext) -> int:
        value = await ctx.port(Ports.commands).receive()
        calls.append(value)
        await ctx.port(Ports.events).emit(value)
        return value

    async with duron.SQLiteScheduler(
        path, workflows=[flow], namespace="commands"
    ) as first:
        with pytest.raises(StorageError, match="already owned"):
            async with duron.SQLiteScheduler(path, namespace="other"):
                pass
        one = await first.submit(flow.ref, "other")
        two = await first.open(flow.ref, "other")
        await one.port(Ports.commands).send(7, idempotency_key="seven")
        assert await asyncio.wait_for(two.result(), 3) == 7
        assert [value async for value in two.port(Ports.events)] == [7]
    async with duron.SQLiteScheduler(path, namespace="other") as other:
        cancelled = await other.submit(flow.ref, "other")
        await cancelled.cancel()
        with pytest.raises(RunCancelledError):
            await cancelled.result()
    async with duron.SQLiteScheduler(
        path, workflows=[flow], namespace="commands"
    ) as host:
        restored = await host.open(flow, "other")
        assert await restored.result() == 7
        await restored.port(Ports.commands).send(7, idempotency_key="seven")
    assert calls == [7]


async def test_reader_drains_outputs_created_during_terminal_observation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release = asyncio.Event()

    @duron.effect
    async def barrier(_ctx: duron.EffectContext) -> None:
        await release.wait()

    @duron.workflow(ports=Ports)
    async def flow(ctx: duron.WorkflowContext) -> None:
        await ctx.call(barrier)
        await ctx.port(Ports.events).emit(42)

    async with duron.SQLiteScheduler(":memory:", workflows=[flow]) as scheduler:
        run = await scheduler.submit(flow, "outputs")

        async def entries(
            reader: EventReader, run_id: str, kind: str, key: str | None = None
        ) -> list[tuple[int, Entry]]:
            result = await history_entries(reader, run_id, kind, key)
            if kind == "stream.create" and not result:
                release.set()
                await asyncio.wait_for(run.result(), 3)
            return result

        monkeypatch.setattr(run_host, "entries", entries)
        values = [value async for value in run.port(Ports.events)]
        assert values == [42]


async def test_candidate_pages_advance_past_unavailable_jobs(tmp_path: Path) -> None:
    @duron.workflow_ref(name="unavailable")
    async def unavailable(_ctx: duron.WorkflowContext) -> int:
        raise NotImplementedError

    @duron.workflow
    async def flow(_ctx: duron.WorkflowContext) -> int:
        return 42

    path = tmp_path / "pages.db"
    async with duron.SQLiteScheduler(path, namespace="other") as scheduler:
        for i in range(65):
            await scheduler.submit(unavailable, f"unavailable-{i:03}")
        run = await scheduler.open(unavailable, "unavailable-000")
        async with asyncio.timeout(5):
            while (await run.status()).state is not duron.RunState.BLOCKED:  # ruff: ignore[async-busy-wait]  # Poll public status.
                await asyncio.sleep(0.01)
    async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
        for i in range(65):
            await scheduler.submit(unavailable, f"unavailable-{i:03}")
        run = await scheduler.submit(flow, "z-runnable")
        assert await asyncio.wait_for(run.result(), 5) == 42
    async with duron.SQLiteScheduler(path, namespace="other") as scheduler:
        run = await scheduler.open(unavailable, "unavailable-000")
        async with asyncio.timeout(5):
            while (await run.status()).state is not duron.RunState.BLOCKED:  # ruff: ignore[async-busy-wait]  # Startup dispatch is asynchronous.
                await asyncio.sleep(0.01)


async def test_process_crash_recovers_committed_effect_immediately(
    tmp_path: Path,
) -> None:
    async with crash_process(tmp_path) as process:
        assert process.stdout is not None
        assert await asyncio.wait_for(process.stdout.readline(), 5) == b"ready\n"
        with pytest.raises(StorageError, match="already owned"):
            async with duron.SQLiteScheduler(tmp_path / "crash.db"):
                pass

    async with duron.SQLiteScheduler(
        tmp_path / "crash.db", workflows=[crash_flow]
    ) as scheduler:
        run = await scheduler.open(crash_flow, "crash")
        await asyncio.wait_for(run.port(CrashPorts.resume).send(3), 5)
        assert await asyncio.wait_for(run.result(), 5) == 10
        assert [value async for value in run.port(CrashPorts.ready)] == [7]
    assert (tmp_path / "calls").read_text() == "effect\n"


async def test_process_crash_blocks_unfinished_effect(tmp_path: Path) -> None:
    async with crash_process(tmp_path, "unfinished") as process:
        assert process.stdout is not None
        assert await asyncio.wait_for(process.stdout.readline(), 5) == b"started\n"

    async with duron.SQLiteScheduler(
        tmp_path / "crash.db", workflows=[unfinished_flow]
    ) as scheduler:
        run = await scheduler.open(unfinished_flow, "unfinished")
        with pytest.raises(
            RunFailedError, match="interrupted before its outcome"
        ) as blocked:
            await asyncio.wait_for(run.result(), 5)
        assert blocked.value.kind is FailureKind.INTERRUPTED_EFFECT
        assert (await run.status()).state is duron.RunState.BLOCKED
    assert (tmp_path / "unfinished-calls").read_text() == "effect\n"


async def test_application_cancellation_does_not_stop_scheduling() -> None:
    @duron.effect
    async def cancel(_ctx: duron.EffectContext) -> int:
        raise asyncio.CancelledError

    @duron.workflow
    async def flow(ctx: duron.WorkflowContext, *, through_effect: bool) -> int:
        if through_effect:
            return await ctx.call(cancel)
        raise asyncio.CancelledError

    @duron.workflow
    async def healthy(_ctx: duron.WorkflowContext) -> int:
        return 42

    async with duron.SQLiteScheduler(
        ":memory:", workflows=[flow, healthy]
    ) as scheduler:
        for through_effect in (False, True):
            run = await scheduler.submit(
                flow, str(through_effect), through_effect=through_effect
            )
            with pytest.raises(RunCancelledError):
                await asyncio.wait_for(run.result(), 3)
            assert (await run.status()).state is duron.RunState.CANCELLED
        run = await scheduler.submit(healthy, "healthy")
        assert await asyncio.wait_for(run.result(), 3) == 42


async def test_invalid_results_are_final_and_typed_boundaries_reject_values(
    tmp_path: Path,
) -> None:
    calls: list[bool] = []

    @duron.effect
    async def broken(_ctx: duron.EffectContext) -> int:
        calls.append(True)
        return cast("int", cast("object", "wrong"))

    @duron.workflow(ports=Ports)
    async def flow(ctx: duron.WorkflowContext) -> int:
        await ctx.port(Ports.commands).receive()
        return await ctx.call(broken)

    path = tmp_path / "invalid.db"
    async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
        run = await scheduler.submit(flow, "invalid")
        with pytest.raises(TypeError, match="declared type"):
            await run.port(Ports.commands).send(cast("int", cast("object", "wrong")))
        await run.port(Ports.commands).send(1)
        with pytest.raises(ApplicationError, match="declared type") as caught:
            await asyncio.wait_for(run.result(), 3)
        assert caught.value.type_name == "builtins:TypeError"
    async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
        run = await scheduler.open(flow, "invalid")
        with pytest.raises(ApplicationError, match="declared type"):
            await run.result()
    assert calls == [True]


async def test_cursors_are_bound_to_persisted_runs(tmp_path: Path) -> None:
    @duron.workflow(ports=Ports)
    async def flow(ctx: duron.WorkflowContext) -> int:
        for i in range(4):
            await ctx.port(Ports.events).emit(i)
        return 4

    path = tmp_path / "cursors.db"
    async with duron.SQLiteScheduler(
        path, workflows=[flow], namespace="first"
    ) as scheduler:
        first = await scheduler.submit(flow, "same")
        assert await first.result() == 4
    async with duron.SQLiteScheduler(
        path, workflows=[flow], namespace="second"
    ) as scheduler:
        second = await scheduler.submit(flow, "same")
        assert await second.result() == 4
        reader = second.port(Ports.events).reader()
        assert [value async for value in reader] == [0, 1, 2, 3]
        cursor = reader.cursor
    async with duron.SQLiteScheduler(path, namespace="first") as scheduler:
        first = await scheduler.open(flow, "same")
        with pytest.raises(ValueError, match="cursor"):
            first.port(Ports.events).reader(after=cursor)
    async with duron.SQLiteScheduler(
        tmp_path / "other.db", workflows=[flow], namespace="second"
    ) as scheduler:
        other = await scheduler.submit(flow, "same")
        with pytest.raises(ValueError, match="cursor"):
            other.port(Ports.events).reader(after=cursor)


async def test_unexpected_dispatcher_exit_is_supervised(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = SQLiteScheduler._job_candidates
    failures: list[bool] = []

    async def candidates(self: SQLiteScheduler, after: str) -> Sequence[ScheduledRun]:
        if not failures:
            failures.append(True)
            msg = "unexpected scheduler failure"
            raise RuntimeError(msg)
        return await original(self, after)

    monkeypatch.setattr(SQLiteScheduler, "_job_candidates", candidates)

    @duron.workflow
    async def flow(_ctx: duron.WorkflowContext) -> int:
        return 42

    async with duron.SQLiteScheduler(":memory:", workflows=[flow]) as scheduler:
        run = await scheduler.submit(flow, "supervised")
        assert await asyncio.wait_for(run.result(), 3) == 42
    assert failures == [True]


async def test_rejected_open_does_not_register_incompatible_implementation() -> None:
    @duron.workflow(name="shared")
    async def correct(_ctx: duron.WorkflowContext, value: int) -> int:
        return value

    @duron.workflow(name="shared")
    async def incompatible(_ctx: duron.WorkflowContext, value: str) -> str:
        return value

    async with SQLiteScheduler(":memory:") as scheduler:
        original = await scheduler.submit(correct.ref, "original", 1)
        with pytest.raises(ContractMismatchError):
            await scheduler.open(incompatible, "original")
        next_run = await scheduler.submit(correct, "next", 2)
        assert await next_run.result() == 2
        assert await original.result() == 1


async def test_submission_identity_observers_and_terminal_results(
    tmp_path: Path,
) -> None:
    calls: list[int] = []

    @duron.effect
    async def double(_ctx: duron.EffectContext, value: int) -> int:
        calls.append(value)
        await asyncio.sleep(0.02)
        return value * 2

    @duron.workflow(name="double")
    async def flow(
        ctx: duron.WorkflowContext, values: list[int], *, increment: int = 1
    ) -> int:
        return await ctx.call(double, sum(values)) + increment

    path = tmp_path / "scheduler.db"
    async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
        values = [2, 3]
        run = await scheduler.submit(flow, "same", values)
        assert_type(run, duron.Run[int, duron.NoPorts])
        values.append(100)
        retry = await scheduler.submit(flow, "same", [2, 3], increment=1)
        with pytest.raises(SubmissionConflictError):
            await scheduler.submit(flow.ref, "same", values)
        waiter = asyncio.create_task(run.result())
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert await asyncio.wait_for(retry.result(), 3) == 11
        assert (await run.status()).state is duron.RunState.SUCCEEDED
    async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
        run = await scheduler.open(flow, "same")
        assert await run.result() == 11
        await run.cancel()
        assert await run.result() == 11
    assert calls == [5]


async def test_input_acceptance_recovery_and_output_reconnect(tmp_path: Path) -> None:
    class Foreign(duron.PortSet):
        events = duron.Output[int]()

    @duron.workflow(ports=Ports)
    async def flow(ctx: duron.WorkflowContext) -> int:
        assert_type(ctx.port(Ports.commands), duron.Receiver[int])
        assert_type(ctx.port(Ports.events), duron.Emitter[int])
        with pytest.raises(WorkflowDefinitionError, match="undeclared"):
            ctx.port(Foreign.events)
        first = await ctx.port(Ports.commands).receive()
        await ctx.port(Ports.events).emit(first)
        second = await ctx.port(Ports.commands).receive()
        await ctx.port(Ports.events).emit(second)
        with pytest.raises(PortClosedError):
            await ctx.port(Ports.commands).receive()
        return first

    path = tmp_path / "ports.db"
    async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
        run = await scheduler.submit(flow.ref, "ports")
        assert_type(run, duron.Run[int, Ports])
        with pytest.raises(WorkflowDefinitionError, match="undeclared"):
            run.port(Foreign.events)
        sender = run.port(Ports.commands)
        await sender.send(3, idempotency_key="first")
        await sender.send(4, idempotency_key="second")
        await sender.close()
        await sender.send(3, idempotency_key="first")
        with pytest.raises(PostConflictError):
            await sender.send(8, idempotency_key="first")
        reader = run.port(Ports.events).reader()
        assert await anext(reader) == 3
        cursor = reader.cursor
    async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
        run = await scheduler.open(flow.ref, "ports")
        assert await asyncio.wait_for(run.result(), 3) == 3
        assert [
            value async for value in run.port(Ports.events).reader(after=cursor)
        ] == [4]
        future = OutputCursor(cursor.stream, cursor.offset + 10000)
        assert (
            await asyncio.wait_for(
                anext(run.port(Ports.events).reader(after=future), None), 3
            )
            is None
        )
        await run.port(Ports.commands).send(4, idempotency_key="second")
        await run.port(Ports.commands).send(5)
        other = await scheduler.submit(flow.ref, "other")
        with pytest.raises(ValueError, match="cursor"):
            other.port(Ports.events).reader(after=cursor)


async def test_unknown_contract_blocks_until_registered_and_cancel_cleans_up(
    tmp_path: Path,
) -> None:
    @duron.workflow_ref(name="shared")
    async def reference(_ctx: duron.WorkflowContext, value: int) -> int:
        raise AssertionError(value)

    cleanup = asyncio.Event()
    started = asyncio.Event()

    @duron.effect
    async def wait(_ctx: duron.EffectContext, value: int) -> int:
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleanup.set()
        return value

    @duron.workflow(name="shared")
    async def implementation(ctx: duron.WorkflowContext, value: int) -> int:
        return await ctx.call(wait, value)

    path = tmp_path / "shared.db"
    async with duron.SQLiteScheduler(path, workflows=[]) as scheduler:
        run = await scheduler.submit(reference, "blocked", 7)
        for _ in range(100):
            if (await run.status()).state is duron.RunState.BLOCKED:
                break
            await asyncio.sleep(0.01)
        assert (await run.status()).state is duron.RunState.BLOCKED
        assert (await run.status()).detail == "implementation unavailable"
    async with duron.SQLiteScheduler(path, workflows=[implementation]) as scheduler:
        run = await scheduler.open(reference, "blocked")
        await scheduler.submit(implementation, "blocked", 7)
        await asyncio.wait_for(started.wait(), 3)
        await run.cancel()
        await run.cancel()
        with pytest.raises(RunCancelledError):
            await asyncio.wait_for(run.result(), 3)
        assert cleanup.is_set()


async def test_schema_mismatch_is_explicit() -> None:
    @duron.workflow(name="same")
    async def implementation(_ctx: duron.WorkflowContext, value: int) -> int:
        return value

    @duron.workflow_ref(name="same")
    async def incompatible(_ctx: duron.WorkflowContext, value: str) -> int:
        raise AssertionError(value)

    async with duron.SQLiteScheduler(
        ":memory:", workflows=[implementation]
    ) as scheduler:
        with pytest.raises(ContractMismatchError):
            await scheduler.submit(incompatible, "bad", "text")


async def test_queued_cancellation_and_terminal_failure_are_final(
    tmp_path: Path,
) -> None:
    calls: list[bool] = []

    @duron.workflow(name="cancel.before.start")
    async def flow(_ctx: duron.WorkflowContext) -> int:
        calls.append(True)
        msg = "business failure"
        raise ValueError(msg)

    path = tmp_path / "cancel.db"
    async with duron.SQLiteScheduler(path, workflows=[]) as scheduler:
        run = await scheduler.submit(flow.ref, "cancelled")
        await run.cancel()
        with pytest.raises(RunCancelledError):
            await run.result()
    assert calls == []
    async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
        run = await scheduler.submit(flow.ref, "failure")
        with pytest.raises(ApplicationError, match="business failure"):
            await asyncio.wait_for(run.result(), 3)
        await run.cancel()
        with pytest.raises(ApplicationError, match="business failure"):
            await run.result()
    async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
        run = await scheduler.open(flow.ref, "failure")
        with pytest.raises(ApplicationError, match="business failure"):
            await run.result()
    assert calls == [True]


@pytest.mark.parametrize(
    ("failure", "kind", "attempts"),
    [
        (StorageError("disk"), FailureKind.INFRASTRUCTURE, 3),
        (ValueError("bad input"), FailureKind.INVALID, 1),
    ],
)
async def test_job_failures_are_typed_and_retried_by_kind(
    monkeypatch: pytest.MonkeyPatch,
    failure: Exception,
    kind: FailureKind,
    attempts: int,
) -> None:
    loads: list[int] = []

    async def load(*_args: object, **_kwargs: object) -> object:
        loads.append(1)
        raise failure

    monkeypatch.setattr(RunMachine, "load", load)

    @duron.workflow
    async def flow(_ctx: duron.WorkflowContext) -> int:
        return 1

    async with duron.SQLiteScheduler(":memory:", workflows=[flow]) as scheduler:
        run = await scheduler.submit(flow, "failing")
        with pytest.raises(RunFailedError) as caught:
            await asyncio.wait_for(run.result(), 5)
        assert caught.value.kind is kind
        assert len(loads) == attempts
        assert (await run.status()).state is duron.RunState.FAILED


async def test_registration_wakes_blocked_run_and_drains_delivery_batches(
    tmp_path: Path,
) -> None:
    @duron.workflow(ports=Ports)
    async def flow(ctx: duron.WorkflowContext) -> int:
        total = 0
        for _ in range(70):
            total += await ctx.port(Ports.commands).receive()
        await ctx.port(Ports.events).emit(total)
        return total

    async with (
        asyncio.timeout(5),
        duron.SQLiteScheduler(tmp_path / "batches.db") as scheduler,
    ):
        run = await scheduler.submit(flow.ref, "batch")
        with pytest.raises(RunFailedError):
            await run.result()
        for value in range(70):
            await run.port(Ports.commands).send(value)
        restored = await scheduler.open(flow, "batch")
        outputs = restored.port(Ports.events).reader()
        result = asyncio.create_task(restored.result())
        assert await anext(outputs) == sum(range(70))
        assert await result == sum(range(70))
        assert [value async for value in outputs] == []
