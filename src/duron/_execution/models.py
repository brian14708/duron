"""Immutable effect identity, invocation context, and execution policies."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Generic, Literal, cast, overload
from typing_extensions import TypeVar

from duron._execution.validation import validate_schema
from duron._ref import hash_id
from duron.errors import InvalidHandleError, WorkflowDefinitionError

if TYPE_CHECKING:
    from collections.abc import Mapping

    from duron._api.ports import EffectEmitter, EffectReceiver, Input, Output, _Port
    from duron._execution.handles import HandleResolver
    from duron._hints import JSONValue, TypeHint

_T = TypeVar("_T")
HandleDirection = Literal["stream.sender", "signal.sender", "signal.receiver"]


def direction_claims_channel(direction: HandleDirection) -> bool:
    """Whether an owner of this direction claims and closes the channel.

    ``signal.sender`` handles share their channel with the receiver scope:
    they neither claim exclusive ownership nor close it. Every other direction
    claims its channel for one owner and is closed when that owner finishes.

    Returns:
        Whether the direction carries channel-ownership semantics.

    """
    return direction != "signal.sender"


def _identity(value: object, name: str) -> None:
    if not isinstance(value, str) or not value:
        msg = f"{name} must be a nonempty UTF-8 string"
        raise InvalidHandleError(msg)
    try:
        _ = value.encode("utf-8")
    except UnicodeError as error:
        msg = f"{name} must be valid UTF-8"
        raise InvalidHandleError(msg) from error


@dataclass(frozen=True, slots=True)
class StreamId:
    run_seed: str
    value: str

    def __post_init__(self) -> None:
        _identity(self.run_seed, "run seed")
        _identity(self.value, "stream identity")


@dataclass(frozen=True, slots=True)
class StreamCursor:
    stream: StreamId
    offset: int

    def __post_init__(self) -> None:
        if self.offset < 0:
            msg = "a stream cursor requires a stream identity and committed offset"
            raise InvalidHandleError(msg)


@dataclass(frozen=True, slots=True)
class StreamEvent(Generic[_T]):
    value: _T
    cursor: StreamCursor


def _parameter(name: object, item_type: object) -> None:
    _identity(name, "parameter name")
    try:
        validate_schema(item_type)
    except TypeError as error:
        msg = "handle parameters require a specified supported payload type"
        raise InvalidHandleError(msg) from error


@dataclass(frozen=True, slots=True)
class OutputPortParameter(Generic[_T]):
    name: str
    item_type: TypeHint[_T]

    def __post_init__(self) -> None:
        _parameter(self.name, self.item_type)


@dataclass(frozen=True, slots=True)
class InputPortParameter(Generic[_T]):
    name: str
    item_type: TypeHint[_T]

    def __post_init__(self) -> None:
        _parameter(self.name, self.item_type)


@dataclass(frozen=True, slots=True)
class HandleReference:
    """Authority-free reference resolved by the current execution owner."""

    run_seed: str
    channel_id: str
    direction: HandleDirection
    owner_id: str
    schema_id: str
    format_version: Literal[0] = 0

    def __post_init__(self) -> None:
        for name, value in (
            ("run seed", self.run_seed),
            ("channel identity", self.channel_id),
            ("owner identity", self.owner_id),
            ("payload schema identity", self.schema_id),
        ):
            _identity(value, name)
        if (
            type(self.format_version) is not int
            or cast("object", self.format_version) != 0
            or not isinstance(cast("object", self.direction), str)
            or self.direction
            not in {"stream.sender", "signal.sender", "signal.receiver"}
        ):
            msg = "unsupported handle reference version or direction"
            raise InvalidHandleError(msg)

    def fields(self) -> dict[str, JSONValue]:
        return {
            "format_version": self.format_version,
            "run_seed": self.run_seed,
            "channel_id": self.channel_id,
            "direction": self.direction,
            "owner_id": self.owner_id,
            "schema_id": self.schema_id,
        }

    @classmethod
    def from_fields(cls, value: object) -> HandleReference:
        fields = _fields(
            value,
            {
                "format_version",
                "run_seed",
                "channel_id",
                "direction",
                "owner_id",
                "schema_id",
            },
        )
        # The constructor validates every field before the reference is exposed.
        return cls(
            cast("str", fields["run_seed"]),
            cast("str", fields["channel_id"]),
            cast("HandleDirection", fields["direction"]),
            cast("str", fields["owner_id"]),
            cast("str", fields["schema_id"]),
            cast("Literal[0]", fields["format_version"]),
        )


def _fields(value: object, keys: set[str]) -> dict[str, object]:
    if not isinstance(value, dict) or set(cast("dict[object, object]", value)) != keys:
        msg = "invalid handle descriptor fields"
        raise InvalidHandleError(msg)
    return cast("dict[str, object]", value)


def _empty_port_bindings() -> dict[
    _Port, EffectReceiver[object] | EffectEmitter[object]
]:
    return {}


def canonical_json(value: JSONValue) -> bytes:
    """Encode the version-0 canonical JSON representation.

    Returns:
        Deterministic UTF-8 bytes with finite JSON values.

    """
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def digest(domain: str, value: JSONValue) -> str:
    return hash_id(domain.encode("ascii") + b"\0" + canonical_json(value))


@dataclass(frozen=True, slots=True)
class ExecutionIdentity:
    """One durable effect call."""

    run_seed: str
    operation_id: str
    effect_name: str


@dataclass(frozen=True, slots=True)
class EffectContextImpl:
    """Identity supplied to an effect body.

    Effects return or raise; completion authority is owned by the runtime.
    """

    identity: ExecutionIdentity
    handles: HandleResolver | None = field(default=None, repr=False, compare=False)
    _port_bindings: Mapping[_Port, EffectReceiver[object] | EffectEmitter[object]] = (
        field(default_factory=_empty_port_bindings, repr=False, compare=False)
    )

    @overload
    def port(self, declaration: Input[_T], /) -> EffectReceiver[_T]: ...
    @overload
    def port(self, declaration: Output[_T], /) -> EffectEmitter[_T]: ...
    def port(
        self, declaration: Input[_T] | Output[_T], /
    ) -> EffectReceiver[_T] | EffectEmitter[_T]:
        try:
            endpoint = self._port_bindings[declaration]
        except KeyError:
            msg = f"effect port {declaration!r} is not bound to this invocation"
            raise WorkflowDefinitionError(msg) from None
        return cast("EffectReceiver[_T] | EffectEmitter[_T]", endpoint)


@dataclass(frozen=True, slots=True)
class EffectOperation:
    """Resolved, snapshotted call handed from the public API to the session."""

    name: str
    seed: str
    inputs: bytes
    handles: tuple[HandleReference, ...]
