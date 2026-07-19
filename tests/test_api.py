from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast
from typing_extensions import Any, TypeVar, override

import pytest

import duron
from duron._api.run import OutputReader
from duron._core.utils import decode_error, encode_error
from duron.contrib.codecs import PickleCodec, PydanticCodec
from duron.contrib.storage import MemoryStorage

if TYPE_CHECKING:
    from duron.log import Entry

_V = TypeVar("_V")

# --- Ports shared across tests ---------------------------------------------

events = duron.Output[str]("events")
user_input = duron.Input[str]("user_input")
cancel_signal = duron.Signal[str]("cancel")
approval = duron.Request[str, bool]("approval")


@duron.effect
async def double(x: int) -> int:
    await asyncio.sleep(0.001)
    return x * 2


@dataclass
class _Point:
    x: int
    y: int


@dataclass
class _NullablePayload:
    a: int
    b: int | None = 5


async def _aiter(*items: _V) -> AsyncIterator[_V]:
    for item in items:
        yield item


def _collect(
    storage: MemoryStorage, workflow: duron.Workflow[..., Any], port: duron.Output[_V]
) -> OutputReader[_V]:
    return OutputReader(storage, workflow.codec, port, terminal=lambda: True)


async def test_basic_workflow_start_and_result() -> None:
    @duron.workflow
    async def greet(ctx: duron.WorkflowContext, x: int) -> int:
        return await ctx.call(double, x)

    assert await duron.run(greet(21), MemoryStorage()) == 42
    assert greet.name.endswith("greet")


async def test_run_context_yields_handle_and_waits_on_clean_exit() -> None:
    finished = asyncio.Event()

    @duron.effect
    async def finish() -> int:
        await asyncio.sleep(0.01)
        finished.set()
        return 7

    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> int:
        return await ctx.call(finish)

    async with duron.run(wf(), MemoryStorage()) as run:
        assert isinstance(run, duron.Run)

    assert finished.is_set()
    assert await run.result() == 7


async def test_run_context_exception_leaves_run_resumable() -> None:
    value = duron.Input[str]("resume_value")
    storage = MemoryStorage()

    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> str:
        return await ctx.receive(value)

    async def fail_host() -> None:
        async with duron.run(wf(), storage):
            msg = "host failed"
            raise RuntimeError(msg)

    with pytest.raises(RuntimeError, match="host failed"):
        await fail_host()

    async with duron.run(wf(), storage) as resumed:
        await resumed.input(value).send("ok")
        assert await resumed.result() == "ok"


async def test_rerun_reuses_completed_run() -> None:
    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> int:
        return await ctx.call(double, 1)

    storage = MemoryStorage()
    assert await duron.run(wf(), storage) == 2
    # A second run over the same storage resumes the recorded history and
    # returns the recorded result instead of failing.
    assert await duron.run(wf(), storage) == 2


async def test_run_starts_fresh_on_empty_storage() -> None:
    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> int:
        return await ctx.call(double, 1)

    assert await duron.run(wf(), MemoryStorage()) == 2


async def test_get_or_start_input_mismatch() -> None:
    @duron.workflow(name="w", version="1")
    async def wf(ctx: duron.WorkflowContext, x: int) -> int:
        return await ctx.call(double, x)

    storage = MemoryStorage()
    assert await duron.run(wf(5), storage) == 10
    with pytest.raises(duron.HistoryMismatchError):
        await duron.run(wf(6), storage)
    assert await duron.run(wf(5), storage) == 10


async def test_attach_version_mismatch() -> None:
    @duron.workflow(name="w", version="1")
    async def v1(ctx: duron.WorkflowContext) -> int:
        return await ctx.call(double, 1)

    @duron.workflow(name="w", version="2")
    async def v2(ctx: duron.WorkflowContext) -> int:
        return await ctx.call(double, 1)

    storage = MemoryStorage()
    await duron.run(v1(), storage)
    with pytest.raises(duron.HistoryMismatchError):
        await duron.run(v2(), storage)


async def test_outputs_read_from_start() -> None:
    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> None:
        for i in range(3):
            await ctx.emit(events, f"e{i}")

    storage = MemoryStorage()
    await duron.run(wf(), storage)
    assert await _collect(storage, wf, events).collect() == ["e0", "e1", "e2"]


async def test_output_cursor_offset() -> None:
    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> None:
        for i in range(4):
            await ctx.emit(events, f"e{i}")

    storage = MemoryStorage()
    await duron.run(wf(), storage)
    reader = _collect(storage, wf, events)
    assert await reader.next() == "e0"
    checkpoint = reader.offset
    assert checkpoint > 0
    assert await reader.collect() == ["e1", "e2", "e3"]
    assert reader.offset > checkpoint


async def test_wired_output_can_checkpoint_and_resume_after_offset() -> None:
    port = duron.Output[str]("checkpointed_events")

    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> None:
        await ctx.emit(port, "first")
        await ctx.emit(port, "second")

    seen: list[tuple[int, str]] = []

    async def record(entries: AsyncIterator[tuple[int, str]]) -> None:
        async for offset, value in entries:
            seen.append((offset, value))

    storage = MemoryStorage()
    await duron.run(wf().output(port, record), storage)
    assert [value for _offset, value in seen] == ["first", "second"]

    # A restart-safe consumer skips entries at or before its saved offset.
    checkpoint = seen[-1][0]
    replayed: list[tuple[int, str]] = []

    async def resume(entries: AsyncIterator[tuple[int, str]]) -> None:
        async for offset, value in entries:
            if offset > checkpoint:
                replayed.append((offset, value))

    await duron.run(wf().output(port, resume), storage)
    assert replayed == []


async def test_output_reader_rescans_after_observing_terminal_snapshot() -> None:
    port = duron.Output[str]("snapshot_race")

    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> None:
        pass

    class SnapshotStorage:
        def __init__(self) -> None:
            self.entries: list[Any] = [
                {
                    "id": "stream",
                    "ts": 1,
                    "type": "stream.create",
                    "source": "task",
                    "name": "__output__:snapshot_race",
                }
            ]
            self.terminal = False
            self.scans = 0

        async def stream(
            self, *, offset: int | None = None
        ) -> AsyncIterator[tuple[int, Any]]:
            start = 0 if offset is None else offset + 1
            snapshot = self.entries[start:]
            self.scans += 1
            if self.scans == 1:
                self.entries.append({
                    "id": "emit",
                    "ts": 2,
                    "type": "stream.emit",
                    "source": "task",
                    "stream_id": "stream",
                    "value": "final",
                })
                self.terminal = True
            for index, entry in enumerate(snapshot, start=start):
                yield (index, entry)

    storage = SnapshotStorage()
    reader = OutputReader(
        cast("Any", storage), wf.codec, port, terminal=lambda: storage.terminal
    )
    assert await reader.collect() == ["final"]


async def test_inputs_receive_fifo() -> None:
    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> list[str]:
        a = await ctx.receive(user_input)
        b = await ctx.receive(user_input)
        return [a, b]

    result = await duron.run(
        wf().feed(user_input, _aiter("first", "second")), MemoryStorage()
    )
    assert result == ["first", "second"]


async def test_receive_many() -> None:
    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> list[str]:
        collected: list[str] = []
        while len(collected) < 2:
            collected.extend(await ctx.receive_many(user_input))
        return collected

    result = await duron.run(wf().feed(user_input, _aiter("a", "b")), MemoryStorage())
    assert sorted(result) == ["a", "b"]


async def test_signal_interrupt() -> None:
    @duron.effect
    async def forever() -> str:
        await asyncio.sleep(100)
        return "done"

    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> str:
        try:
            async with ctx.interruptible(cancel_signal):
                return await ctx.call(forever)
        except duron.Interrupted as exc:
            return f"interrupted:{exc.value}"

    async with duron.run(wf(), MemoryStorage()) as run:
        await asyncio.sleep(0.02)
        await run.signal(cancel_signal).send("stop")
        assert await run.result() == "interrupted:stop"


async def test_requests_respond() -> None:
    @duron.workflow
    async def wf(ctx: duron.WorkflowContext, tid: str) -> str:
        ok = await ctx.request(approval, tid)
        return "approved" if ok else "rejected"

    async with duron.run(wf("t-1"), MemoryStorage()) as run:
        req = await run.requests(approval).next()
        assert req.value == "t-1"
        assert await req.is_pending() is True
        await req.respond(True)
        assert await req.is_pending() is False
        assert await run.result() == "approved"


async def test_request_already_resolved() -> None:
    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> bool:
        return await ctx.request(approval, "x")

    async with duron.run(wf(), MemoryStorage()) as run:
        req = await run.requests(approval).next()
        await req.respond(True)
        with pytest.raises(duron.RequestAlreadyResolvedError):
            await req.respond(False)
        assert await run.result() is True


async def test_streaming_effect() -> None:
    tokens = duron.Output[str]("tokens")

    @duron.effect
    async def gen(text: str) -> AsyncIterator[str]:
        for ch in text:
            await asyncio.sleep(0.001)
            yield ch

    @duron.workflow
    async def wf(ctx: duron.WorkflowContext, text: str) -> int:
        async with ctx.stream(gen, text) as call:
            async for tok in call:
                await ctx.emit(tokens, tok)
            return await call.result()

    storage = MemoryStorage()
    assert await duron.run(wf("abcd"), storage) == 4
    assert await _collect(storage, wf, tokens).collect() == ["a", "b", "c", "d"]


async def test_streaming_effect_early_exit_cancels_worker() -> None:
    stopped = asyncio.Event()

    @duron.effect
    async def unbounded() -> AsyncIterator[int]:
        try:
            yield 1
            await asyncio.Event().wait()
        finally:
            stopped.set()

    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> int:
        async with ctx.stream(unbounded) as call:
            async for item in call:
                return item
        return 0

    storage = MemoryStorage()
    result = await asyncio.wait_for(duron.run(wf(), storage), timeout=1)
    assert result == 1
    await asyncio.wait_for(stopped.wait(), timeout=1)
    assert await asyncio.wait_for(duron.run(wf(), storage), timeout=1) == 1


async def test_streaming_effect_surfaces_original_error_and_replays() -> None:
    @duron.effect
    async def broken() -> AsyncIterator[int]:
        yield 1
        msg = "stream failed"
        raise ValueError(msg)

    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> str:
        try:
            async with ctx.stream(broken) as call:
                _ = [item async for item in call]
        except ValueError as exc:
            return str(exc)
        return "not handled"

    storage = MemoryStorage()
    assert await duron.run(wf(), storage) == "stream failed"
    assert await duron.run(wf(), storage) == "stream failed"


async def test_crash_and_resume() -> None:
    checkpoint = duron.Output[str]("checkpoint")
    go = duron.Input[str]("go")

    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> int:
        a = await ctx.call(double, 5)
        await ctx.emit(checkpoint, "a")
        _ = await ctx.receive(go)
        return await ctx.call(double, a)

    storage = MemoryStorage()
    reached = asyncio.Event()

    async def on_checkpoint(entries: AsyncIterator[tuple[int, str]]) -> None:
        async for _entry in entries:
            reached.set()

    # Launch, run until the checkpoint emits, then "crash" by cancelling.
    task = asyncio.ensure_future(
        duron.run(wf().output(checkpoint, on_checkpoint), storage)
    )
    await reached.wait()
    _ = task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Resume on the same storage and let it finish.
    result = await duron.run(wf().feed(go, _aiter("continue")), storage)
    assert result == 20


async def test_replay_is_deterministic() -> None:
    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> list[int]:
        return [ctx.random().randint(1, 100), await ctx.call(double, 3)]

    storage = MemoryStorage()
    first = await duron.run(wf(), storage)
    assert await duron.run(wf(), storage) == first


async def test_declared_effect_error_roundtrips() -> None:
    class DeclinedError(Exception):
        pass

    @duron.effect(raises=(DeclinedError,))
    async def boom() -> None:
        msg = "no"
        raise DeclinedError(msg)

    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> str:
        try:
            await ctx.call(boom)
        except DeclinedError:
            return "declined"
        return "not handled"

    storage = MemoryStorage()
    assert await duron.run(wf(), storage) == "declined"
    assert await duron.run(wf(), storage) == "declined"


async def test_decode_error_resolves_declared_types() -> None:
    class DeclinedError(Exception):
        pass

    encoded = encode_error(DeclinedError("nope"))
    expected_key = (
        f"{__name__}:test_decode_error_resolves_declared_types.<locals>.DeclinedError"
    )
    assert encoded.get("key") == expected_key

    # Without a declaration the recorded error stays opaque.
    opaque = decode_error(encoded)
    assert isinstance(opaque, duron.RemoteEffectError)
    assert str(opaque) == "nope"

    # Declaring the type reconstructs the original class.
    decoded = decode_error(encoded, (DeclinedError,))
    assert isinstance(decoded, DeclinedError)
    assert str(decoded) == "nope"


async def test_error_type_cause_chain_roundtrips() -> None:
    inner = ValueError("inner")
    outer = KeyError("outer")
    outer.__cause__ = inner

    decoded = decode_error(encode_error(outer))
    assert isinstance(decoded, KeyError)
    assert isinstance(decoded.__cause__, ValueError)
    assert str(decoded.__cause__) == "inner"


async def test_legacy_builtin_error_without_key_roundtrips() -> None:
    decoded = decode_error({
        "type": "ValueError",
        "module": "builtins",
        "message": "legacy",
        "args": ["legacy"],
        "cancelled": False,
    })
    assert isinstance(decoded, ValueError)
    assert str(decoded) == "legacy"


async def test_sync_effect_requires_executor() -> None:
    def sync_effect(x: int) -> int:
        return x

    with pytest.raises(duron.WorkflowDefinitionError):
        _ = duron.effect(sync_effect)


async def test_sync_effect_thread_executor() -> None:
    @duron.effect(executor="thread")
    def upper(text: str) -> str:
        return text.upper()

    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> str:
        return await ctx.call(upper, "hi")

    assert await duron.run(wf(), MemoryStorage()) == "hi".upper()


async def test_idempotency_key_stable_on_replay() -> None:
    keys: list[str] = []

    @duron.effect
    async def record(key: str) -> str:
        return key

    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> str:
        key = ctx.idempotency_key
        keys.append(key)
        return await ctx.call(record, key)

    storage = MemoryStorage()
    first = await duron.run(wf(), storage)
    replayed = await duron.run(wf(), storage)
    assert first == replayed
    assert keys[0] == first


async def test_deterministic_now_and_sleep() -> None:
    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> float:
        await ctx.sleep(0.01)
        now = await ctx.now()
        return now.timestamp()

    storage = MemoryStorage()
    first = await duron.run(wf(), storage)
    assert await duron.run(wf(), storage) == first


async def test_pydantic_codec_request() -> None:
    @dataclass
    class Approval:
        transfer_id: str

    port = duron.Request[Approval, bool]("pyd_approval")

    @duron.workflow(codec=PydanticCodec())
    async def wf(ctx: duron.WorkflowContext, tid: str) -> str:
        ok = await ctx.request(port, Approval(transfer_id=tid))
        return "ok" if ok else "no"

    async with duron.run(wf("t-9"), MemoryStorage()) as run:
        req = await run.requests(port).next()
        assert req.value.transfer_id == "t-9"
        await req.respond(True)
        assert await run.result() == "ok"


async def test_send_to_never_opened_port_does_not_hang() -> None:
    unused = duron.Input[int]("never_opened")

    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> int:
        return await ctx.call(double, 1)

    async with duron.run(wf(), MemoryStorage()) as run:
        # The workflow never opens `unused`; a send must fail once the run is
        # terminal rather than hang forever.
        with pytest.raises(duron.InvalidRunStateError):
            await asyncio.wait_for(run.input(unused).send(5), timeout=2)
        assert await run.result() == 2


async def test_same_name_ports_do_not_collide() -> None:
    dup_out = duron.Output[str]("dup")
    dup_in = duron.Input[str]("dup")

    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> None:
        value = await ctx.receive(dup_in)
        await ctx.emit(dup_out, f"got:{value}")

    storage = MemoryStorage()
    await duron.run(wf().feed(dup_in, _aiter("hello")), storage)
    assert await _collect(storage, wf, dup_out).collect() == ["got:hello"]


async def test_call_rejects_generator_effect() -> None:
    @duron.effect
    async def gen() -> AsyncIterator[int]:
        yield 1

    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> int:
        return await ctx.call(gen)  # type: ignore[arg-type]

    with pytest.raises(duron.WorkflowDefinitionError):
        await duron.run(wf(), MemoryStorage())


async def test_workflow_rejects_variadic_parameters() -> None:
    async def wf(ctx: duron.WorkflowContext, *items: int) -> int:  # noqa: ARG001
        return sum(items)

    with pytest.raises(duron.WorkflowDefinitionError):
        _ = duron.workflow(wf)


async def test_reader_next_then_collect_continues_from_cursor() -> None:
    port = duron.Output[int]("cursor_nums")

    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> None:
        for i in range(4):
            await ctx.emit(port, i)

    storage = MemoryStorage()
    await duron.run(wf(), storage)
    reader = _collect(storage, wf, port)
    assert await reader.next() == 0
    assert await reader.collect() == [1, 2, 3]


async def test_receive_many_non_blocking_after_close_returns_empty() -> None:
    port = duron.Input[str]("closable")

    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> list[str]:
        got: list[str] = []
        while len(got) < 2:
            got.extend(await ctx.receive_many(port))
        # The port is closed and drained; a non-blocking poll must report
        # "nothing available" ([]) rather than raising PORT_CLOSED.
        await ctx.sleep(0.001)
        tail = await ctx.receive_many(port, wait=False)
        return got + tail

    # feed() closes the input writer once the source is exhausted.
    result = await duron.run(wf().feed(port, _aiter("a", "b")), MemoryStorage())
    assert sorted(result) == ["a", "b"]


async def test_effect_raises_rejects_non_exception_types() -> None:
    async def noop() -> None:
        pass

    with pytest.raises(duron.WorkflowDefinitionError, match="non-exception"):
        _ = duron.effect(raises=("nope",))(noop)  # type: ignore[arg-type]


async def test_explicit_port_types() -> None:
    out = duron.Output("out", str)
    inp = duron.Input("inp", int)
    req = duron.Request("req", request_type=str, response_type=bool)
    assert out.item_type is str
    assert inp.item_type is int
    assert req.request_type is str
    assert req.response_type is bool


async def test_calling_effect_directly_in_workflow_is_rejected() -> None:
    @duron.effect
    async def side_effect() -> int:
        return 1

    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> int:  # noqa: ARG001
        result: int = await side_effect()  # bug: bypasses ctx.call
        return result

    with pytest.raises(duron.WorkflowDefinitionError):
        await duron.run(wf(), MemoryStorage())


async def test_calling_effect_directly_outside_workflow_is_allowed() -> None:
    @duron.effect
    async def side_effect() -> int:
        return 7

    assert await side_effect() == 7


async def test_effect_identity_is_persisted_and_version_checked() -> None:
    calls: list[str] = []

    @duron.effect(name="effects.lookup", version="1")
    async def lookup_v1() -> str:
        calls.append("v1")
        return "one"

    @duron.workflow(name="identity_workflow", version="1")
    async def first(ctx: duron.WorkflowContext) -> str:
        return await ctx.call(lookup_v1)

    storage = MemoryStorage()
    assert await duron.run(first(), storage) == "one"
    entries = await storage.entries()
    assert any(
        entry.get("effect_name") == "effects.lookup"
        and entry.get("effect_version") == "1"
        for entry in entries
    )

    @duron.effect(name="effects.lookup", version="2")
    async def lookup_v2() -> str:
        calls.append("v2")
        return "two"

    @duron.workflow(name="identity_workflow", version="1")
    async def second(ctx: duron.WorkflowContext) -> str:
        return await ctx.call(lookup_v2)

    with pytest.raises(duron.HistoryMismatchError):
        await duron.run(second(), storage)
    assert calls == ["v1"]


async def test_streaming_effect_uses_public_identity() -> None:
    @duron.effect(name="effects.tokens", version="1")
    async def tokens_v1() -> AsyncIterator[str]:
        yield "one"

    @duron.workflow(name="stream_identity_workflow", version="1")
    async def first(ctx: duron.WorkflowContext) -> list[str]:
        async with ctx.stream(tokens_v1) as call:
            return [item async for item in call]

    storage = MemoryStorage()
    assert await duron.run(first(), storage) == ["one"]
    entries = await storage.entries()
    assert any(
        entry.get("type") == "stream.create"
        and entry.get("name") == "effects.tokens"
        and entry.get("effect_name") == "effects.tokens"
        and entry.get("effect_version") == "1"
        for entry in entries
    )

    @duron.effect(name="effects.tokens", version="2")
    async def tokens_v2() -> AsyncIterator[str]:
        yield "two"

    @duron.workflow(name="stream_identity_workflow", version="1")
    async def second(ctx: duron.WorkflowContext) -> list[str]:
        async with ctx.stream(tokens_v2) as call:
            return [item async for item in call]

    with pytest.raises(duron.HistoryMismatchError):
        await duron.run(second(), storage)


async def test_workflow_arguments_are_bound_and_normalized_before_run() -> None:
    @duron.workflow(name="normalized_inputs")
    async def wf(ctx: duron.WorkflowContext, x: int, scale: int = 2) -> int:
        return await ctx.call(double, x) * scale

    storage = MemoryStorage()
    assert await duron.run(wf(3), storage) == 12
    assert await duron.run(wf(x=3), storage) == 12
    assert await duron.run(wf(3, scale=2), storage) == 12

    empty = MemoryStorage()
    with pytest.raises(TypeError):
        await duron.run(wf(), empty)  # type: ignore[call-arg]
    assert await empty.entries() == []
    with pytest.raises(TypeError):
        wf(1, x=1)  # type: ignore[misc]


async def test_incompatible_header_codec_is_history_mismatch() -> None:
    @duron.workflow(name="codec_history", codec=PickleCodec())
    async def pickled(ctx: duron.WorkflowContext) -> int:  # noqa: ARG001
        return 1

    @duron.workflow(name="codec_history")
    async def plain(ctx: duron.WorkflowContext) -> int:  # noqa: ARG001
        return 1

    storage = MemoryStorage()
    assert await duron.run(pickled(), storage) == 1
    with pytest.raises(duron.HistoryMismatchError):
        await duron.run(plain(), storage)


async def test_malformed_persisted_header_is_history_mismatch() -> None:
    @duron.workflow(name="malformed_header")
    async def wf(_ctx: duron.WorkflowContext) -> int:
        return 1

    storage = MemoryStorage()
    lease = await storage.acquire_lease()
    await storage.append(
        lease,
        {
            "id": "complete",
            "ts": 1,
            "type": "promise.complete",
            "source": "effect",
            "promise_id": "prelude",
            "error": encode_error(ValueError("broken header")),
        },
    )
    await storage.release_lease(lease)

    with pytest.raises(duron.HistoryMismatchError):
        await duron.run(wf(), storage)


async def test_stale_runtime_lease_is_storage_error() -> None:
    class LoseLeaseStorage(MemoryStorage):
        def __init__(self) -> None:
            super().__init__()
            self.appends = 0

        @override
        async def append(self, token: bytes, entry: Entry) -> int:
            self.appends += 1
            if self.appends == 2:
                _ = await self.acquire_lease()
            return await super().append(token, entry)

    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> int:  # noqa: ARG001
        return 1

    with pytest.raises(duron.LeaseLostError):
        await duron.run(wf(), LoseLeaseStorage())


async def test_terminal_output_sink_failure_propagates() -> None:
    port = duron.Output[str]("failing_sink")

    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> int:
        await ctx.emit(port, "final")
        return 7

    async def fail(entries: AsyncIterator[tuple[int, str]]) -> None:
        async for _entry in entries:
            msg = "delivery failed"
            raise RuntimeError(msg)

    with pytest.raises(RuntimeError, match="delivery failed"):
        async with duron.run(wf().output(port, fail), MemoryStorage()) as run:
            await run.result()


# --- behaviors preserved from the pre-redesign core suite ------------------


async def test_replay_determinism_with_concurrent_effects() -> None:
    @duron.effect
    async def unique() -> str:
        await asyncio.sleep(0.001)
        return str(uuid.uuid4())

    @duron.workflow
    async def wf(ctx: duron.WorkflowContext, prefix: str) -> str:
        a, b = await asyncio.gather(ctx.call(unique), ctx.call(unique))
        await ctx.sleep(0.01)
        return prefix + f"{a}:{b}"

    storage = MemoryStorage()
    first = await duron.run(wf("p"), storage)
    second = await duron.run(wf("p"), storage)
    assert first == second


async def test_effect_error_propagates_and_replays() -> None:
    @duron.effect
    async def boom() -> None:
        msg = "kaboom"
        raise ValueError(msg)

    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> str:
        try:
            await ctx.call(boom)
        except ValueError:
            return "handled"
        return "unhandled"

    storage = MemoryStorage()
    assert await duron.run(wf(), storage) == "handled"
    assert await duron.run(wf(), storage) == "handled"


async def test_unregistered_effect_error_becomes_remote_error() -> None:
    @duron.effect
    async def boom() -> None:
        class LocalError(Exception):
            pass

        msg = "remote"
        raise LocalError(msg)

    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> None:
        await ctx.call(boom)

    with pytest.raises(duron.RemoteEffectError):
        await duron.run(wf(), MemoryStorage())


async def test_custom_result_codec() -> None:
    @duron.effect
    async def make() -> _Point:
        return _Point(1, 2)

    @duron.workflow(codec=PickleCodec())
    async def wf(ctx: duron.WorkflowContext) -> _Point:
        pt = await ctx.call(make)
        return _Point(pt.x + 5, pt.y + 10)

    assert await duron.run(wf(), MemoryStorage()) == _Point(6, 12)


async def test_concurrent_request_resolution_is_atomic() -> None:
    port = duron.Request[str, int]("atomic")

    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> int:
        return await ctx.request(port, "go")

    winners: list[int] = []

    async with duron.run(wf(), MemoryStorage()) as run:
        req = await run.requests(port).next()

        async def resolve(value: int) -> object:
            try:
                await req.respond(value)
            except duron.DuronError as exc:
                return exc
            return value

        outcomes = await asyncio.gather(resolve(1), resolve(2))
        winners.extend(o for o in outcomes if isinstance(o, int))
        result = await run.result()
    assert len(winners) == 1
    assert result == winners[0]


async def test_external_input_summation() -> None:
    numbers = duron.Input[int]("numbers")

    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> int:
        total = 0
        while True:
            value = await ctx.receive(numbers)
            if value < 0:
                return total
            total += value

    result = await duron.run(
        wf().feed(numbers, _aiter(*range(10), -1)), MemoryStorage()
    )
    assert result == sum(range(10))


async def test_external_output_streaming() -> None:
    values = duron.Output[int]("values")

    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> int:
        for i in range(10):
            await ctx.emit(values, i)
        return 42

    collected: list[int] = []

    async def collect(entries: AsyncIterator[tuple[int, int]]) -> None:
        async for _offset, value in entries:
            collected.append(value)

    storage = MemoryStorage()
    result = await duron.run(wf().output(values, collect), storage)
    assert result == 42
    assert collected == list(range(10))


async def test_time_determinism() -> None:
    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> list[float]:
        async def sample() -> float:
            await ctx.sleep(ctx.random().random() * 0.01)
            return (await ctx.now()).timestamp()

        return await asyncio.gather(*[sample() for _ in range(5)])

    storage = MemoryStorage()
    first = await duron.run(wf(), storage)
    assert await duron.run(wf(), storage) == first


async def test_invocation_is_single_use() -> None:
    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> int:
        return await ctx.call(double, 2)

    invocation = wf()
    assert await duron.run(invocation, MemoryStorage()) == 4
    with pytest.raises(RuntimeError, match="single-use"):
        await duron.run(invocation, MemoryStorage())


async def test_sleep_resumes_remaining_deadline() -> None:
    started = duron.Output[str]("sleep_started")

    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> str:
        await ctx.emit(started, "begin")
        await ctx.sleep(0.25)
        return "woke"

    storage = MemoryStorage()
    reached = asyncio.Event()

    async def on_started(entries: AsyncIterator[tuple[int, str]]) -> None:
        async for _entry in entries:
            reached.set()

    task = asyncio.ensure_future(duron.run(wf().output(started, on_started), storage))
    await reached.wait()
    await asyncio.sleep(0.05)  # crash mid-sleep
    _ = task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Let the recorded deadline pass, then resume: only the remainder (here,
    # none) is slept, instead of the full 0.25s starting over.
    await asyncio.sleep(0.25)
    begin = time.monotonic()
    assert await duron.run(wf(), storage) == "woke"
    assert time.monotonic() - begin < 0.2


async def test_request_fail_requires_declared_error_type() -> None:
    class UndeclaredError(Exception):
        pass

    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> bool:
        return await ctx.request(approval, "x")

    async with duron.run(wf(), MemoryStorage()) as run:
        req = await run.requests(approval).next()
        with pytest.raises(duron.UndeclaredErrorTypeError):
            await req.fail(UndeclaredError("nope"))
        await req.respond(True)
        assert await run.result() is True


async def test_request_fail_with_declared_error_type_roundtrips() -> None:
    class DeclinedError(Exception):
        pass

    port = duron.Request[str, bool]("declinable", raises=(DeclinedError,))

    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> str:
        try:
            _ = await ctx.request(port, "x")
        except DeclinedError as exc:
            return f"declined:{exc}"
        return "not handled"

    storage = MemoryStorage()
    async with duron.run(wf(), storage) as run:
        req = await run.requests(port).next()
        await req.fail(DeclinedError("no"))
        assert await run.result() == "declined:no"

    # Replay resolves the recorded failure against the port's declared errors.
    assert await duron.run(wf(), storage) == "declined:no"


# --- Regression tests for review fixes ---------------------------------------


async def test_stream_call_second_iteration_terminates() -> None:
    @duron.effect
    async def gen(text: str) -> AsyncIterator[str]:
        for ch in text:
            yield ch

    @duron.workflow
    async def wf(ctx: duron.WorkflowContext, text: str) -> list[list[str]]:
        async with ctx.stream(gen, text) as call:
            first = [item async for item in call]
            # A drained call terminates immediately instead of waiting forever
            # on a reader that will never wake again.
            second = [item async for item in call]
        return [first, second]

    result = await asyncio.wait_for(duron.run(wf("ab"), MemoryStorage()), timeout=2)
    assert result == [["a", "b"], []]


async def test_stream_call_result_after_early_exit_raises() -> None:
    @duron.effect
    async def unbounded() -> AsyncIterator[int]:
        yield 1
        await asyncio.Event().wait()

    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> str:
        call = ctx.stream(unbounded)
        async with call:
            async for _item in call:
                break
        try:
            _ = await call.result()
        except duron.InvalidRunStateError:
            return "invalid_run_state"
        return "no error"

    result = await asyncio.wait_for(duron.run(wf(), MemoryStorage()), timeout=2)
    assert result == "invalid_run_state"


async def test_stream_rejects_non_generator_effect() -> None:
    @duron.effect
    async def not_a_gen(x: int) -> int:
        return x

    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> int:
        async with ctx.stream(not_a_gen, 5):  # type: ignore[arg-type]
            return 0

    with pytest.raises(duron.WorkflowDefinitionError):
        _ = await duron.run(wf(), MemoryStorage())


async def test_stop_iteration_effect_error_does_not_hang() -> None:
    @duron.effect(executor="inline")
    def exhausted() -> int:
        msg = "boom"
        raise StopIteration(msg)

    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> int:
        return await ctx.call(exhausted)

    with pytest.raises(duron.RemoteEffectError):
        _ = await asyncio.wait_for(duron.run(wf(), MemoryStorage()), timeout=2)


async def test_pydantic_codec_preserves_explicit_none() -> None:
    @duron.effect
    async def make() -> _NullablePayload:
        return _NullablePayload(a=1, b=None)

    @duron.workflow(codec=PydanticCodec())
    async def wf(ctx: duron.WorkflowContext) -> _NullablePayload:
        return await ctx.call(make)

    result = await duron.run(wf(), MemoryStorage())
    assert result.b is None


async def test_pydantic_codec_untyped_effect_roundtrips() -> None:
    @duron.effect
    async def untyped(x: int) -> Any:  # noqa: ANN401
        return {"x": x}

    @duron.workflow(codec=PydanticCodec())
    async def wf(ctx: duron.WorkflowContext) -> int:
        payload = await ctx.call(untyped, 3)
        return cast("dict[str, int]", payload)["x"]

    assert await duron.run(wf(), MemoryStorage()) == 3


def test_legacy_format0_error_entries_decode() -> None:
    # Version-0 logs written before the structured error encoding are still
    # accepted by header validation, so their {"code": int} entries must keep
    # decoding usefully: -2 marked cancellation.
    cancelled = decode_error(cast("Any", {"code": -2, "message": "CancelledError()"}))
    assert isinstance(cancelled, asyncio.CancelledError)

    failed = decode_error(cast("Any", {"code": -1, "message": "ValueError('x')"}))
    assert isinstance(failed, duron.RemoteEffectError)


async def test_undeclared_error_type_decodes_as_remote_error() -> None:
    class FirstError(Exception):
        pass

    class SecondError(Exception):
        pass

    # The recorded key names FirstError exactly, so declaring an unrelated
    # class cannot capture it.
    encoded = encode_error(FirstError("nope"))
    decoded = decode_error(encoded, (SecondError,))
    assert isinstance(decoded, duron.RemoteEffectError)
    assert not isinstance(decoded, SecondError)


async def test_feed_unconsumed_signal_does_not_mask_result() -> None:
    unused_sig = duron.Signal[str]("unused_signal")

    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> str:
        _ = await ctx.call(double, 2)
        return "ok"

    # The feeder blocks on a signal the workflow never arms and fails when the
    # run turns terminal; that must not mask the workflow's own result.
    result = await asyncio.wait_for(
        duron.run(wf().feed(unused_sig, _aiter("x")), MemoryStorage()), timeout=2
    )
    assert result == "ok"


async def test_declared_error_without_matching_signature_roundtrips() -> None:
    """A declared error type whose __init__ takes no parameters still arrives as
    itself on the first execution.

    Reconstruction rebuilds the exception from the recorded ``args`` alone, and
    an appended entry is fed straight back through the engine, so a type whose
    constructor rejects those positional args used to decode as
    RemoteEffectError even with no crash and no replay involved.
    """

    class DeclinedError(Exception):
        def __init__(self) -> None:
            super().__init__("declined")

    class InsufficientFundsError(Exception):
        def __init__(self, shortfall: int) -> None:
            super().__init__("insufficient funds")
            self.shortfall = shortfall

    @duron.effect(raises=(DeclinedError, InsufficientFundsError))
    async def decline() -> None:
        raise DeclinedError

    @duron.effect(raises=(DeclinedError, InsufficientFundsError))
    async def withdraw() -> None:
        raise InsufficientFundsError(42)

    caught: list[str] = []

    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> str:
        # Reconstruction rebuilds from the recorded ``args`` alone, so only the
        # type and the message survive; attributes a constructor derived from
        # its parameters are not restored.
        try:
            await ctx.call(decline)
        except DeclinedError:
            caught.append("declined")

        try:
            await ctx.call(withdraw)
        except InsufficientFundsError:
            caught.append("withdraw")

        return "both round-tripped" if len(caught) == 2 else f"not caught: {caught}"

    # A single, uninterrupted run over fresh storage: no crash, no replay.
    storage = MemoryStorage()
    assert await duron.run(wf(), storage) == "both round-tripped"
    assert caught == ["declined", "withdraw"], caught
    caught.clear()
    assert await duron.run(wf(), storage) == "both round-tripped"
    assert caught == ["declined", "withdraw"], caught


async def test_undeclared_builtin_error_needs_no_declaration() -> None:
    """Built-in subclasses round-trip without being listed in ``raises``."""

    class AppError(Exception):
        pass

    port = duron.Request[str, str]("builtin_fail")

    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> str:
        try:
            return await ctx.request(port, "q")
        except FileNotFoundError as exc:
            return f"FileNotFoundError: {exc}"
        except duron.RemoteEffectError as exc:
            return f"opaque: {exc.type_name}"

    storage = MemoryStorage()
    async with duron.run(wf(), storage) as run:
        async for pending in run.requests(port):
            await pending.fail(FileNotFoundError("missing"))
            break

    assert await duron.run(wf(), storage) == "FileNotFoundError: missing"
    assert await duron.run(wf(), storage) == "FileNotFoundError: missing"

    # A user-defined type still has to be declared to fail with.
    app_port = duron.Request[str, int]("app_fail")
    with pytest.raises(duron.UndeclaredErrorTypeError):
        await _fail_request(app_port, AppError("boom"))


async def _fail_request(port: duron.Request[str, int], error: Exception) -> None:
    @duron.workflow
    async def wf(ctx: duron.WorkflowContext) -> int:
        return await ctx.request(port, "q")

    async with duron.run(wf(), MemoryStorage()) as run:
        async for pending in run.requests(port):
            await pending.fail(error)
            break
