"""Typed scheduler observation and interaction contracts."""

# pyright: reportPrivateUsage=false
# ruff: file-ignore[private-member-access]

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Generic, cast, overload
from typing_extensions import Protocol, TypeVar, override

from duron._api.ports import Input, NoPorts, Output, PortSet, _check_port
from duron._execution.errors import decode_error
from duron._execution.models import canonical_json, digest
from duron._history.pagination import iter_entries
from duron.errors import ContractMismatchError, RunCancelledError
from duron.storage._protocol import validate_run_id

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from duron._api.ports import Reader, Sender, _Port
    from duron._api.reference import WorkflowRef
    from duron._history.entries import RunFinishEntry
    from duron.codec import Codec
    from duron.storage._protocol import EventReader


_T_co = TypeVar("_T_co", covariant=True)
_Ports_co = TypeVar("_Ports_co", bound=PortSet, covariant=True, default=NoPorts)
_V = TypeVar("_V")

__all__ = ()


class RunState(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUSPENDED = "suspended"
    BLOCKED = "blocked"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True, slots=True)
class RunStatus:
    """Observed lifecycle state and the latest scheduling diagnostic."""

    state: RunState
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class OutputCursor:
    """Position in one persisted output stream, independent of display names."""

    stream: str
    offset: int

    def __post_init__(self) -> None:
        if not isinstance(cast("object", self.stream), str) or not self.stream:
            msg = "output cursor stream must be a nonempty identity"
            raise ValueError(msg)
        if type(self.offset) is not int or self.offset < 0:
            msg = "output cursor offset must be a non-negative integer"
            raise ValueError(msg)


class Run(Protocol[_T_co, _Ports_co]):
    """An observation handle whose lifetime does not own execution."""

    @property
    def id(self) -> str: ...
    @property
    def port_set(self) -> type[_Ports_co]: ...
    async def status(self) -> RunStatus: ...
    async def result(self) -> _T_co: ...
    async def cancel(self) -> None: ...

    @overload
    def port(self, declaration: Input[_V], /) -> Sender[_V]: ...
    @overload
    def port(self, declaration: Output[_V], /) -> Reader[_V]: ...


_T = TypeVar("_T")
_Ports = TypeVar("_Ports", bound=PortSet, default=NoPorts)


class _Backend(Protocol):
    async def _wait(self) -> None: ...
    async def _observe(
        self, run_id: str
    ) -> tuple[RunState, RunFinishEntry | None, str | None]: ...
    async def _cancel_run(self, run_id: str) -> None: ...
    async def _deliver(
        self,
        run_id: str,
        kind: str,
        port: str,
        payload: bytes,
        retry_id: str | None = None,
    ) -> None: ...
    def _outputs(
        self,
        run_id: str,
        ref: WorkflowRef[..., Any, Any],
        port: Output[_V],
        after: OutputCursor | None,
        prefetch: int,
        identity: str,
    ) -> _OutputReader[_V]: ...


class _Run(Generic[_T, _Ports]):
    def __init__(
        self,
        backend: _Backend,
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
        state, _outcome, error = await self.backend._observe(self.id)
        return RunStatus(state, error)

    async def result(self) -> _T:
        while True:
            _state, outcome, error = await self.backend._observe(self.id)
            if outcome is not None:
                if outcome["cancelled"]:
                    msg = f"run {self.id!r} was cancelled"
                    raise RunCancelledError(msg)
                if "error" in outcome:
                    raise decode_error(outcome["error"], ())
                assert "result" in outcome
                return self.reference.codec.decode_json(
                    outcome["result"], self.reference.result_type
                )
            if error is not None and error.startswith("contract:"):
                raise ContractMismatchError(error)
            await self.backend._wait()

    async def cancel(self) -> None:
        await self.backend._cancel_run(self.id)

    @overload
    def port(self, declaration: Input[_V], /) -> Sender[_V]: ...
    @overload
    def port(self, declaration: Output[_V], /) -> Reader[_V]: ...
    def port(self, declaration: _Port, /) -> Sender[Any] | Reader[Any]:
        _check_port(declaration, self.reference.ports, self.reference.name)
        if isinstance(declaration, Input):
            return _Input(self, declaration)
        return _Output(self, declaration)


class _Input(Generic[_V]):
    def __init__(self, run: _Run[Any, Any], port: Input[_V]) -> None:
        self.run, self.port = run, port

    async def send(self, value: _V, /, *, idempotency_key: str | None = None) -> None:
        if idempotency_key is not None:
            validate_run_id(idempotency_key)
        payload = canonical_json(
            self.run.reference.codec.encode_json(value, self.port.item_type)
        )
        await self.run.backend._deliver(
            self.run.id, "input", self.port.name, payload, idempotency_key
        )

    async def close(self) -> None:
        await self.run.backend._deliver(self.run.id, "close", self.port.name, b"null")


class _Output(Generic[_V]):
    def __init__(self, run: _Run[Any, Any], port: Output[_V]) -> None:
        self.run, self.port = run, port

    def reader(
        self, *, after: OutputCursor | None = None, prefetch: int = 64
    ) -> _OutputReader[_V]:
        if type(prefetch) is not int or prefetch <= 0:
            msg = "prefetch must be a positive integer"
            raise ValueError(msg)
        return self.run.backend._outputs(
            self.run.id,
            self.run.reference,
            self.port,
            after,
            prefetch,
            digest("duron.output.v0", [self.run.seed, self.port.name]),
        )

    def __aiter__(self) -> AsyncIterator[_V]:
        return self.reader()


class _OutputReader(AsyncIterator[_T]):
    """Read committed output and resume from the last delivered value."""

    def __init__(
        self,
        backend: EventReader,
        codec: Codec,
        port: Output[_T],
        *,
        after: OutputCursor | None = None,
        prefetch: int = 64,
        history_id: str,
        identity: str,
        state: Callable[[], Awaitable[tuple[str | None, bool]]],
        wait: Callable[[], Awaitable[None]],
    ) -> None:
        self._backend = backend
        self._history_id = history_id
        self._codec = codec
        self._port = port
        self._prefetch = prefetch
        self._identity = identity
        self._state = state
        self._wait = wait
        if after is not None and (
            not isinstance(cast("object", after), OutputCursor)
            or after.stream != identity
        ):
            msg = "output cursor belongs to a different run or output port"
            raise ValueError(msg)
        self._offset = after.offset if after is not None else 0
        self._iterator = self._iterate()

    @property
    def cursor(self) -> OutputCursor:
        """The position of the last value delivered by this reader."""
        return OutputCursor(self._identity, self._offset)

    @override
    def __aiter__(self) -> _OutputReader[_T]:
        return self

    @override
    async def __anext__(self) -> _T:
        return await anext(self._iterator)

    async def _iterate(self) -> AsyncIterator[_T]:
        scanned = self._offset
        while True:
            stream, terminal = await self._state()
            if stream is not None:
                async for offset, entry in iter_entries(
                    self._backend,
                    self._history_id,
                    after=scanned,
                    page_size=self._prefetch,
                    stream_id=stream,
                ):
                    scanned = offset
                    if entry["type"] in {"stream.complete", "stream.scope.exit"}:
                        return
                    if entry["type"] == "stream.emit":
                        value = self._codec.decode_json(
                            entry["value"], self._port.item_type
                        )
                        self._offset = offset
                        yield value
            if terminal:
                return
            await self._wait()
