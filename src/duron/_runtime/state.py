"""Run-owned operation and channel state, reconstructed from committed history."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING

from duron._runtime.channels import OwnedChannelState
from duron._runtime.ops import EffectCall

if TYPE_CHECKING:
    from duron._history.entries import PromiseCreateEntry
    from duron._runtime.loop import OpFuture
    from duron._runtime.ops import FnCall, WaitCall


class Phase(Enum):
    REPLAYING = auto()
    LIVE = auto()
    FINISHED = auto()
    CLOSED = auto()


@dataclass(slots=True)
class Operation:
    """An admitted operation; worker state never substitutes for durable state."""

    parameters: FnCall[object] | WaitCall[object] | EffectCall[object]
    waiter: OpFuture
    committed: PromiseCreateEntry | None = None
    started: bool = False
    reporting: bool = False
    waiting_version: int | None = None

    @property
    def effect(self) -> EffectCall[object] | None:
        return self.parameters if isinstance(self.parameters, EffectCall) else None


@dataclass(slots=True)
class MachineState:
    run_id: str
    seed: str
    phase: Phase = Phase.REPLAYING
    frontier: int = -1
    append_version: int = 0
    operations: dict[str, Operation] = field(default_factory=dict[str, Operation])
    channels: dict[str, OwnedChannelState] = field(
        default_factory=dict[str, OwnedChannelState]
    )
    channel_claims: dict[str, str] = field(default_factory=dict[str, str])
    closed_inputs: set[str] = field(default_factory=set[str])
    channel_gate: asyncio.Lock = field(default_factory=asyncio.Lock)
