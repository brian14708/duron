"""Shared codecs, execution, and process helpers for runtime regressions."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import sys
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, cast
from typing_extensions import ParamSpec, TypeVar

from duron._api.run import Run, RunState
from duron._execution.errors import decode_error
from duron._runtime.history import read_outcome
from duron._runtime.machine import RunMachine
from duron.errors import ClaimBusyError

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from pathlib import Path

    from duron._api.ports import PortSet
    from duron._api.workflow import Workflow
    from duron.codec import JSONValue, TypeHint
    from duron.storage import EventStore


_P = ParamSpec("_P")
_T = TypeVar("_T")
_Ports = TypeVar("_Ports", bound="PortSet")


class IntegerCodec:
    @staticmethod
    def encode_json(value: object, annotation: TypeHint[object]) -> JSONValue:
        assert annotation is int
        assert type(value) is int
        return f"integer:{value}"

    @staticmethod
    def decode_json(encoded: JSONValue, annotation: TypeHint[_T]) -> _T:
        assert annotation is int
        assert isinstance(encoded, str)
        assert encoded.startswith("integer:")
        return cast("_T", int(encoded.removeprefix("integer:")))


async def wait_suspended(run: Run[_T, _Ports]) -> None:
    async with asyncio.timeout(3):
        while (await run.status()).state is not RunState.SUSPENDED:  # ruff: ignore[async-busy-wait]  # Public status polling.
            await asyncio.sleep(0.002)


async def assert_unloaded(store: EventStore, run_id: str) -> None:
    async with asyncio.timeout(3):
        while True:
            try:
                token = await store.claim(run_id)
            except ClaimBusyError:
                await asyncio.sleep(0.01)
            else:
                await store.release(run_id, token)
                return


async def _no_deliveries(_execution: RunMachine[_T]) -> None:
    await asyncio.Event().wait()


async def execute(
    workflow: Workflow[_P, _T, _Ports],
    store: EventStore,
    run_id: str,
    *args: _P.args,
    **kwargs: _P.kwargs,
) -> _T:
    async with (
        asyncio.timeout(5),
        await RunMachine.load(
            store,
            workflow,
            run_id,
            workflow.ref._encode(*args, **kwargs),  # ruff: ignore[private-member-access]
        ) as machine,
    ):
        await machine.run(asyncio.Event(), _no_deliveries)
    outcome = await read_outcome(store, run_id)
    assert outcome is not None
    if "error" in outcome:
        raise decode_error(outcome["error"])
    assert "result" in outcome
    return workflow.codec.decode_json(outcome["result"], workflow.ref.result_type)


@asynccontextmanager
async def crash_process(
    directory: Path, *args: str
) -> AsyncGenerator[asyncio.subprocess.Process]:
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "tests.crash_worker",
        str(directory),
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        yield process
    finally:
        if process.returncode is None:
            process.kill()
        await process.wait()
