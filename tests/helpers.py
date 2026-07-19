"""Instrumentation helpers for private replay-engine regression tests."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING
from typing_extensions import ParamSpec, TypeVar

from duron._api.run import Run, RunState
from duron._execution.errors import decode_error
from duron._history.encoding import random_id
from duron._runtime.history import read_outcome
from duron._runtime.machine import RunMachine
from duron.errors import ClaimBusyError

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from duron._api.ports import PortSet
    from duron._api.workflow import Workflow
    from duron.storage import EventStore


_P = ParamSpec("_P")
_T = TypeVar("_T")
_Ports = TypeVar("_Ports", bound="PortSet")


async def wait_suspended(run: Run[_T, _Ports]) -> None:
    async with asyncio.timeout(3):
        while (await run.status()).state is not RunState.SUSPENDED:  # ruff: ignore[async-busy-wait]  # Public status polling.
            await asyncio.sleep(0.002)


async def assert_unloaded(store: EventStore, run_id: str) -> None:
    async with asyncio.timeout(3):
        while True:
            try:
                lease = await store.claim(run_id, random_id())
            except ClaimBusyError:
                await asyncio.sleep(0.01)
            else:
                await store.release(run_id, lease)
                return


async def _not_cancelled() -> bool:
    return False


async def _no_deliveries(_execution: RunMachine[object]) -> None:
    await asyncio.Event().wait()


async def execute(
    workflow: Workflow[_P, _T, _Ports],
    store: EventStore,
    run_id: str,
    *args: _P.args,
    **kwargs: _P.kwargs,
) -> _T:
    async with await RunMachine.load(
        store,
        workflow,
        run_id,
        workflow.ref._encode(*args, **kwargs),  # ruff: ignore[private-member-access]
    ) as machine:
        await machine.run(_not_cancelled, _no_deliveries)
    outcome = await read_outcome(store, run_id)
    assert outcome is not None
    if "error" in outcome:
        raise decode_error(outcome["error"], ())
    assert "result" in outcome
    return workflow.codec.decode_json(outcome["result"], workflow.ref.result_type)


@asynccontextmanager
async def host(
    workflow: Workflow[_P, _T, _Ports],
    store: EventStore,
    run_id: str,
    *args: _P.args,
    **kwargs: _P.kwargs,
) -> AsyncGenerator[None]:
    execution = asyncio.create_task(execute(workflow, store, run_id, *args, **kwargs))
    try:
        yield
    finally:
        execution.cancel()
        await asyncio.gather(execution, return_exceptions=True)
