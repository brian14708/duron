# pyright: reportPrivateUsage=false
# ruff: file-ignore[private-member-access]

from __future__ import annotations

import asyncio
import sqlite3
import sys
from contextlib import closing
from typing import TYPE_CHECKING, assert_type, cast

import pytest

import duron
import duron._api.scheduler as scheduler_api
from duron import OutputCursor
from duron._runtime.history import entries as history_entries
from duron.errors import (
    ContractMismatchError,
    PortClosedError,
    PostConflictError,
    RunCancelledError,
    RunNotFoundError,
    StorageError,
    SubmissionConflictError,
    WorkflowDefinitionError,
)
from duron.storage import SQLiteStore
from tests.crash_worker import CrashPorts
from tests.crash_worker import flow as crash_flow
from tests.helpers import assert_unloaded, wait_suspended

if TYPE_CHECKING:
    from pathlib import Path

    from duron._api.scheduler import DeliveryError, ScheduledRun
    from duron._history.entries import Entry
    from duron.storage import LeaseToken
    from duron.storage._protocol import EventReader


class Ports(duron.PortSet):
    commands = duron.Input[int]()
    events = duron.Output[int]()


async def test_sqlite_scheduler_lifecycle_and_shutdown_recovery(tmp_path: Path) -> None:
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
    with pytest.raises(RuntimeError, match="not active"):
        await run.status()
    finish.set()
    async with scheduler:
        restored = await scheduler.open(flow.ref, "recover")
        assert await asyncio.wait_for(restored.result(), 3) == 43

    transient = duron.SQLiteScheduler(":memory:")
    async with transient:
        assert await (await transient.submit(flow, "transient", 0)).result() == 42
    async with transient:
        with pytest.raises(RunNotFoundError):
            await transient.open(flow, "transient")


async def test_sqlite_scheduler_startup_failure_closes_storage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_wake = duron.SQLiteScheduler._job_wake
    original_close = SQLiteStore.close
    closed: list[SQLiteStore] = []

    async def wake(self: duron.SQLiteScheduler) -> None:
        await original_wake(self)
        msg = "startup failed after opening storage"
        raise RuntimeError(msg)

    async def close(self: SQLiteStore) -> None:
        await original_close(self)
        closed.append(self)

    scheduler = duron.SQLiteScheduler(":memory:")
    monkeypatch.setattr(duron.SQLiteScheduler, "_job_wake", wake)
    monkeypatch.setattr(SQLiteStore, "close", close)
    with pytest.raises(RuntimeError, match="startup failed"):
        async with scheduler:
            pytest.fail("startup succeeded")
    assert len(closed) == 1
    monkeypatch.setattr(duron.SQLiteScheduler, "_job_wake", original_wake)
    async with scheduler:
        pass
    assert len(closed) == 2


async def test_structured_jobs_deliveries_and_history(tmp_path: Path) -> None:
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
                "SELECT DISTINCT namespace, job_id, event_type, event_source "
                "FROM log_entries WHERE event_type = 'stream.emit'"
            ).fetchall() == [("app", "one", "stream.emit", "task")]
        await run.port(Ports.commands).send(4, idempotency_key="four")
        assert await run.result() == 7
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute(
            "SELECT state FROM jobs WHERE namespace = 'app' AND run_id = 'one'"
        ).fetchall() == [("succeeded",)]
        assert connection.execute(
            "SELECT namespace, run_id, kind, port, payload, state FROM deliveries"
        ).fetchall() == [("app", "one", "input", "commands", "4", "accepted")]


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
        async with (
            SQLiteStore(path) as store,
            duron.SQLiteScheduler(
                path, workflows=[timer, waiting], idle_timeout=0.01
            ) as scheduler,
        ):
            timed = await scheduler.submit(timer, "timer")
            cancelled = await scheduler.submit(waiting, "cancel")
            await wait_suspended(timed)
            await wait_suspended(cancelled)
            await assert_unloaded(store, '["run","default","timer"]')
            await assert_unloaded(store, '["run","default","cancel"]')
        await asyncio.sleep(0.03)
        async with (
            SQLiteStore(path) as store,
            duron.SQLiteScheduler(
                path, workflows=[timer, waiting], idle_timeout=0.01
            ) as scheduler,
        ):
            cancelled = await scheduler.open(waiting, "cancel")
            assert (await cancelled.status()).state is duron.RunState.SUSPENDED
            await cancelled.cancel()
            with pytest.raises(RunCancelledError):
                await cancelled.result()
            timed = await scheduler.open(timer, "timer")
            assert await anext(timed.port(Ports.events).reader()) == 1
            assert await timed.result() == "timeout"


async def test_delivery_during_suspension_is_not_lost(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    suspending = asyncio.Event()
    posted = asyncio.Event()
    original_suspend = duron.SQLiteScheduler._job_suspend
    original_post = duron.SQLiteScheduler._delivery_post

    async def suspend(
        self: duron.SQLiteScheduler, run_id: str, deadline: float | None
    ) -> bool:
        suspending.set()
        await posted.wait()
        return await original_suspend(self, run_id, deadline)

    async def post(
        self: duron.SQLiteScheduler,
        delivery_id: str,
        run_id: str,
        kind: str,
        port: str,
        payload: bytes,
        retry_id: str | None,
    ) -> None:
        await original_post(self, delivery_id, run_id, kind, port, payload, retry_id)
        posted.set()

    monkeypatch.setattr(duron.SQLiteScheduler, "_job_suspend", suspend)
    monkeypatch.setattr(duron.SQLiteScheduler, "_delivery_post", post)

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
    key = '["run","default","result"]'

    async def release(self: SQLiteStore, run_id: str, lease: LeaseToken) -> None:
        if run_id == key:
            releasing.set()
            await finish.wait()
        await original(self, run_id, lease)

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
    lease = await store.claim(key, "next-owner")
    await store.release(key, lease)
    await store.close()


async def test_shutdown_during_acquisition_releases_run_claim(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    claimed = asyncio.Event()
    finish = asyncio.Event()
    original = SQLiteStore.claim
    key = '["run","default","acquiring"]'

    async def claim(
        self: SQLiteStore, run_id: str, request_id: str, *, ttl: float = 30
    ) -> LeaseToken:
        lease = await original(self, run_id, request_id, ttl=ttl)
        if run_id == key:
            claimed.set()
            await finish.wait()
        return lease

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
            lease = await store.claim(key, "next-owner")
            await store.release(key, lease)


async def test_competing_hosts_and_namespace_keys(tmp_path: Path) -> None:
    path = tmp_path / "namespaces.db"
    calls: list[int] = []

    @duron.workflow(ports=Ports)
    async def flow(ctx: duron.WorkflowContext) -> int:
        value = await ctx.port(Ports.commands).receive()
        calls.append(value)
        await ctx.port(Ports.events).emit(value)
        return value

    # These names used to let a run key collide with another namespace journal.
    async with (
        duron.SQLiteScheduler(path, workflows=[flow], namespace="commands") as first,
        duron.SQLiteScheduler(path, workflows=[flow], namespace="commands") as second,
        duron.SQLiteScheduler(path, namespace="other") as other,
    ):
        one, two = await asyncio.gather(
            first.submit(flow.ref, "other"), second.submit(flow.ref, "other")
        )
        await one.port(Ports.commands).send(7, idempotency_key="seven")
        assert await asyncio.wait_for(two.result(), 3) == 7
        assert [value async for value in two.port(Ports.events)] == [7]
        cancelled = await other.submit(flow.ref, "cancelled")
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


async def test_obsolete_sqlite_schema_is_rejected_without_changes(
    tmp_path: Path,
) -> None:
    path = tmp_path / "obsolete.db"
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute("PRAGMA user_version=99")
        connection.execute("CREATE TABLE scheduler_jobs (run_id TEXT)")
        connection.execute("INSERT INTO scheduler_jobs VALUES ('retained')")
    with pytest.raises(StorageError, match="unsupported SQLite"):
        async with duron.SQLiteScheduler(path):
            pytest.fail("obsolete schema accepted")
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("SELECT * FROM scheduler_jobs").fetchall() == [
            ("retained",)
        ]
        assert connection.execute("PRAGMA user_version").fetchone() == (99,)
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall() == [("scheduler_jobs",)]


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

        monkeypatch.setattr(scheduler_api, "entries", entries)
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
        assert (await run.status()).state is duron.RunState.BLOCKED


async def test_process_crash_recovers_committed_effect_after_lease_expiry(
    tmp_path: Path,
) -> None:
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "tests.crash_worker",
        str(tmp_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        assert process.stdout is not None
        assert await asyncio.wait_for(process.stdout.readline(), 5) == b"ready\n"
    finally:
        if process.returncode is None:
            process.kill()
        await process.wait()

    async with duron.SQLiteScheduler(
        tmp_path / "crash.db", workflows=[crash_flow]
    ) as scheduler:
        run = await scheduler.open(crash_flow, "crash")
        await asyncio.wait_for(run.port(CrashPorts.resume).send(3), 5)
        assert await asyncio.wait_for(run.result(), 5) == 10
        assert [value async for value in run.port(CrashPorts.ready)] == [7]
    assert (tmp_path / "calls").read_text() == "effect\n"


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
        with pytest.raises(TypeError, match="declared type"):
            await asyncio.wait_for(run.result(), 3)
    async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
        run = await scheduler.open(flow, "invalid")
        with pytest.raises(TypeError, match="declared type"):
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
    original = duron.SQLiteScheduler._job_candidates
    failures: list[bool] = []

    async def candidates(self: duron.SQLiteScheduler, after: str) -> list[ScheduledRun]:
        if not failures:
            failures.append(True)
            msg = "unexpected scheduler failure"
            raise RuntimeError(msg)
        return await original(self, after)

    monkeypatch.setattr(duron.SQLiteScheduler, "_job_candidates", candidates)

    @duron.workflow
    async def flow(_ctx: duron.WorkflowContext) -> int:
        return 42

    async with duron.SQLiteScheduler(":memory:", workflows=[flow]) as scheduler:
        run = await scheduler.submit(flow, "supervised")
        assert await asyncio.wait_for(run.result(), 3) == 42
    assert failures == [True]


async def test_accepted_delivery_recovers_after_lost_acknowledgement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    failed = asyncio.Event()
    original = duron.SQLiteScheduler._delivery_acknowledge

    async def acknowledge(
        self: duron.SQLiteScheduler,
        delivery_id: str,
        error: DeliveryError | None = None,
    ) -> None:
        if not failed.is_set():
            failed.set()
            msg = "acceptance committed but acknowledgement was lost"
            raise StorageError(msg)
        await original(self, delivery_id, error)

    monkeypatch.setattr(duron.SQLiteScheduler, "_delivery_acknowledge", acknowledge)

    @duron.workflow(ports=Ports)
    async def flow(ctx: duron.WorkflowContext) -> int:
        value = await ctx.port(Ports.commands).receive()
        with pytest.raises(PortClosedError):
            await ctx.port(Ports.commands).receive()
        return value

    async with duron.SQLiteScheduler(
        tmp_path / "delivery.db", workflows=[flow]
    ) as scheduler:
        run = await scheduler.submit(flow.ref, "delivery")
        await asyncio.wait_for(
            run.port(Ports.commands).send(7, idempotency_key="seven"), 3
        )
        assert failed.is_set()
        await run.port(Ports.commands).send(7, idempotency_key="seven")
        await run.port(Ports.commands).close()
        assert await asyncio.wait_for(run.result(), 3) == 7


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
        with pytest.raises(PortClosedError):
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
        with pytest.raises(ValueError, match="business failure"):
            await asyncio.wait_for(run.result(), 3)
        await run.cancel()
        with pytest.raises(ValueError, match="business failure"):
            await run.result()
    async with duron.SQLiteScheduler(path, workflows=[flow]) as scheduler:
        run = await scheduler.open(flow.ref, "failure")
        with pytest.raises(ValueError, match="business failure"):
            await run.result()
    assert calls == [True]


async def test_competing_hosts_preserve_ownership_and_dispatch_other_jobs(
    tmp_path: Path,
) -> None:
    entered = asyncio.Event()
    finish = asyncio.Event()
    calls: list[int] = []

    @duron.effect
    async def work(_ctx: duron.EffectContext, value: int) -> int:
        calls.append(value)
        if value == 1:
            entered.set()
            await finish.wait()
        return value

    @duron.workflow
    async def flow(ctx: duron.WorkflowContext, value: int) -> int:
        return await ctx.call(work, value)

    path = tmp_path / "owners.db"
    async with duron.SQLiteScheduler(path, workflows=[flow]) as first:
        one = await first.submit(flow.ref, "one", 1)
        await asyncio.wait_for(entered.wait(), 3)
        async with duron.SQLiteScheduler(path, workflows=[flow]) as second:
            two = await second.submit(flow.ref, "two", 2)
            assert await asyncio.wait_for(two.result(), 3) == 2
            finish.set()
            assert await asyncio.wait_for(one.result(), 3) == 1
    assert sorted(calls) == [1, 2]
