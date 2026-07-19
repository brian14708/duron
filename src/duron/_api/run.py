"""Typed scheduler observation and interaction contracts."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, overload
from typing_extensions import Protocol, TypeVar

from duron._api.ports import Input, NoPorts, Output, PortSet

if TYPE_CHECKING:
    from duron._api.ports import Reader, Sender


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
        if not self.stream:
            msg = "output cursor stream must be a nonempty identity"
            raise ValueError(msg)
        if self.offset < 0:
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
