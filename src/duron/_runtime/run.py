"""Implementations of scheduler run handles and output readers."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Generic, overload
from typing_extensions import Protocol, TypeVar, override

from duron._api.ports import Input, Output, PortSet, _check_port
from duron._api.run import OutputCursor, RunState, RunStatus
from duron._execution.errors import decode_error
from duron._execution.models import canonical_json, digest
from duron._history.encoding import iter_entries
from duron.errors import (
    ContractMismatchError,
    FailureKind,
    HistoryMismatchError,
    RunCancelledError,
    RunFailedError,
)
from duron.storage._protocol import validate_id

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from duron._api.ports import PortReader, Reader, Sender
    from duron._api.reference import WorkflowRef
    from duron._history.entries import PromiseCompleteEntry
    from duron.codec import Codec
    from duron.storage._protocol import EventReader

_T = TypeVar("_T")
_Ports = TypeVar("_Ports", bound=PortSet)
_V = TypeVar("_V")


class RunService(Protocol):
    def watch(self, run_id: str) -> Callable[[], Awaitable[None]]: ...
    async def observe(
        self, run_id: str
    ) -> tuple[
        RunState, PromiseCompleteEntry | None, str | None, FailureKind | None
    ]: ...
    async def cancel(self, run_id: str) -> None: ...
    async def deliver(
        self,
        run_id: str,
        kind: str,
        port: str,
        payload: bytes,
        idempotency_key: str | None = None,
    ) -> None: ...
    def outputs(
        self,
        run_id: str,
        ref: WorkflowRef[..., object, PortSet],
        port: Output[_V],
        after: OutputCursor | None,
        prefetch: int,
        identity: str,
    ) -> PortReader[_V]: ...


class RunHandle(Generic[_T, _Ports]):
    def __init__(
        self,
        backend: RunService,
        reference: WorkflowRef[..., _T, _Ports],
        run_id: str,
        seed: str,
    ) -> None:
        self.backend = backend
        self.reference = reference
        self.id = run_id
        self.seed = seed

    @property
    def port_set(self) -> type[_Ports]:
        return self.reference.port_set

    async def status(self) -> RunStatus:
        state, _outcome, error, _kind = await self.backend.observe(self.id)
        return RunStatus(state, error)

    async def result(self) -> _T:
        while True:
            wait = self.backend.watch(self.id)
            state, outcome, error, kind = await self.backend.observe(self.id)
            if outcome is not None:
                if "error" in outcome and outcome["error"].get("cancelled"):
                    msg = f"run {self.id!r} was cancelled"
                    raise RunCancelledError(msg)
                if "error" in outcome:
                    raise decode_error(outcome["error"])
                assert "result" in outcome
                return self.reference.codec.decode_json(
                    outcome["result"], self.reference.result_type
                )
            if error is not None and state in {RunState.FAILED, RunState.BLOCKED}:
                if kind is FailureKind.HISTORY_MISMATCH:
                    raise HistoryMismatchError(error)
                if kind is FailureKind.CONTRACT_MISMATCH:
                    raise ContractMismatchError(error)
                raise RunFailedError(error, kind or FailureKind.INFRASTRUCTURE)
            await wait()

    async def cancel(self) -> None:
        await self.backend.cancel(self.id)

    @overload
    def port(self, declaration: Input[_V], /) -> Sender[_V]: ...
    @overload
    def port(self, declaration: Output[_V], /) -> Reader[_V]: ...
    def port(self, declaration: Input[_V] | Output[_V], /) -> Sender[_V] | Reader[_V]:
        _check_port(declaration, self.reference.ports, self.reference.name)
        if isinstance(declaration, Input):
            return _Input(self, declaration)
        return _Output(self, declaration)


class _Input(Generic[_T, _Ports, _V]):
    def __init__(self, run: RunHandle[_T, _Ports], port: Input[_V]) -> None:
        self.run, self.port = run, port

    async def send(self, value: _V, /, *, idempotency_key: str | None = None) -> None:
        if idempotency_key is not None:
            validate_id(idempotency_key, "idempotency_key")
        payload = canonical_json(
            self.run.reference.codec.encode_json(value, self.port.item_type)
        )
        await self.run.backend.deliver(
            self.run.id, "input", self.port.name, payload, idempotency_key
        )

    async def close(self) -> None:
        await self.run.backend.deliver(self.run.id, "close", self.port.name, b"null")


class _Output(Generic[_T, _Ports, _V]):
    def __init__(self, run: RunHandle[_T, _Ports], port: Output[_V]) -> None:
        self.run, self.port = run, port

    def reader(
        self, *, after: OutputCursor | None = None, prefetch: int = 64
    ) -> PortReader[_V]:
        if type(prefetch) is not int or prefetch <= 0:
            msg = "prefetch must be a positive integer"
            raise ValueError(msg)
        return self.run.backend.outputs(
            self.run.id,
            self.run.reference,
            self.port,
            after,
            prefetch,
            digest("duron.output.v0", [self.run.seed, self.port.name]),
        )

    def __aiter__(self) -> AsyncIterator[_V]:
        return self.reader()


class OutputReader(AsyncIterator[_T]):
    def __init__(
        self,
        backend: EventReader,
        codec: Codec,
        port: Output[_T],
        *,
        after: OutputCursor | None = None,
        prefetch: int = 64,
        run_id: str,
        identity: str,
        state: Callable[[], Awaitable[tuple[str | None, bool]]],
        watch: Callable[[], Callable[[], Awaitable[None]]],
    ) -> None:
        self._backend = backend
        self._run_id = run_id
        self._codec = codec
        self._port = port
        self._prefetch = prefetch
        self._identity = identity
        self._state = state
        self._watch = watch
        if after is not None and after.stream != identity:
            msg = "output cursor belongs to a different run or output port"
            raise ValueError(msg)
        self._offset = after.offset if after is not None else 0
        self._iterator = self._iterate()

    @property
    def cursor(self) -> OutputCursor:
        return OutputCursor(self._identity, self._offset)

    @override
    def __aiter__(self) -> OutputReader[_T]:
        return self

    @override
    async def __anext__(self) -> _T:
        return await anext(self._iterator)

    async def _iterate(self) -> AsyncIterator[_T]:
        scanned = self._offset
        while True:
            wait = self._watch()
            stream, terminal = await self._state()
            if stream is not None:
                async for offset, entry in iter_entries(
                    self._backend,
                    self._run_id,
                    after=scanned,
                    page_size=self._prefetch,
                    stream_id=stream,
                ):
                    scanned = offset
                    if entry["type"] == "stream.complete":
                        return
                    if entry["type"] == "stream.emit":
                        value = self._codec.decode_json(
                            entry["value"], self._port.item_type
                        )
                        self._offset = offset
                        yield value
            if terminal:
                return
            await wait()
