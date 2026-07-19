"""Immutable identities, references, and bounded handle recovery summaries."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, Literal, cast
from typing_extensions import TypeVar

from duron._runtime.execution.value_schema import validate_schema
from duron.errors import InvalidHandleError

if TYPE_CHECKING:
    from duron.types import JSONValue, TypeHint

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


@dataclass(frozen=True, slots=True)
class SignalDelivery:
    """A committed signal record returned by the run owner."""

    message_id: str
    offset: int
    payload: JSONValue


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
class SignalId:
    run_seed: str
    value: str

    def __post_init__(self) -> None:
        _identity(self.run_seed, "run seed")
        _identity(self.value, "signal identity")


@dataclass(frozen=True, slots=True)
class MessageId:
    signal: SignalId
    value: str

    def __post_init__(self) -> None:
        if not isinstance(cast("object", self.signal), SignalId):
            msg = "a message requires a signal identity"
            raise InvalidHandleError(msg)
        _identity(self.value, "message identity")


@dataclass(frozen=True, slots=True)
class StreamCursor:
    stream: StreamId
    offset: int

    def __post_init__(self) -> None:
        if (
            not isinstance(cast("object", self.stream), StreamId)
            or type(self.offset) is not int
            or self.offset < 0
        ):
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
class StreamParameter(Generic[_T]):
    name: str
    item_type: TypeHint[_T]

    def __post_init__(self) -> None:
        _parameter(self.name, self.item_type)


@dataclass(frozen=True, slots=True)
class CommandSenderParameter(Generic[_T]):
    name: str
    item_type: TypeHint[_T]

    def __post_init__(self) -> None:
        _parameter(self.name, self.item_type)


@dataclass(frozen=True, slots=True)
class CommandReceiverParameter(Generic[_T]):
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
    codec_id: str
    format_version: Literal[1] = 1

    def __post_init__(self) -> None:
        for name, value in (
            ("run seed", self.run_seed),
            ("channel identity", self.channel_id),
            ("owner identity", self.owner_id),
            ("payload schema identity", self.schema_id),
            ("codec identity", self.codec_id),
        ):
            _identity(value, name)
        if (
            type(self.format_version) is not int
            or cast("object", self.format_version) != 1
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
            "codec_id": self.codec_id,
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
                "codec_id",
            },
        )
        # The constructor validates every field before the reference is exposed.
        return cls(
            cast("str", fields["run_seed"]),
            cast("str", fields["channel_id"]),
            cast("HandleDirection", fields["direction"]),
            cast("str", fields["owner_id"]),
            cast("str", fields["schema_id"]),
            cast("str", fields["codec_id"]),
            cast("Literal[1]", fields["format_version"]),
        )


@dataclass(frozen=True, slots=True)
class HandleRecovery:
    reference: HandleReference
    latest_offset: int | None = None
    acknowledged_offset: int | None = None
    delivered_offset: int | None = None
    closed: bool = False

    def __post_init__(self) -> None:
        if not isinstance(cast("object", self.reference), HandleReference):
            msg = "handle recovery requires a validated reference"
            raise InvalidHandleError(msg)
        for offset in (
            self.latest_offset,
            self.acknowledged_offset,
            self.delivered_offset,
        ):
            if offset is not None and (type(offset) is not int or offset < 0):
                msg = "handle recovery offsets must be committed log offsets"
                raise InvalidHandleError(msg)
        if type(self.closed) is not bool:
            msg = "handle recovery closure must be a boolean"
            raise InvalidHandleError(msg)
        if self.delivered_offset is not None and (
            self.reference.direction == "stream.sender"
            or self.latest_offset is None
            or self.delivered_offset > self.latest_offset
            or (
                self.acknowledged_offset is not None
                and self.delivered_offset <= self.acknowledged_offset
            )
        ):
            msg = "invalid signal delivery recovery cursor"
            raise InvalidHandleError(msg)
        if self.acknowledged_offset is not None and (
            self.reference.direction == "stream.sender"
            or self.latest_offset is None
            or self.acknowledged_offset > self.latest_offset
        ):
            msg = "invalid signal acknowledgement recovery cursor"
            raise InvalidHandleError(msg)

    def fields(self) -> dict[str, JSONValue]:
        return {
            "reference": self.reference.fields(),
            "latest_offset": self.latest_offset,
            "acknowledged_offset": self.acknowledged_offset,
            "delivered_offset": self.delivered_offset,
            "closed": self.closed,
        }

    @classmethod
    def from_fields(cls, value: object) -> HandleRecovery:
        fields = _fields(
            value,
            {
                "reference",
                "latest_offset",
                "acknowledged_offset",
                "delivered_offset",
                "closed",
            },
        )
        return cls(
            HandleReference.from_fields(fields["reference"]),
            cast("int | None", fields["latest_offset"]),
            cast("int | None", fields["acknowledged_offset"]),
            cast("int | None", fields["delivered_offset"]),
            cast("bool", fields["closed"]),
        )


@dataclass(frozen=True, slots=True)
class RecoveryDescriptor:
    """One constant-size summary per declared handle, never event history."""

    handles: tuple[HandleRecovery, ...] = ()
    format_version: Literal[1] = 1

    def __post_init__(self) -> None:
        if (
            type(self.format_version) is not int
            or cast("object", self.format_version) != 1
            or not isinstance(cast("object", self.handles), tuple)
            or any(
                not isinstance(handle, HandleRecovery)
                for handle in cast("tuple[object, ...]", self.handles)
            )
        ):
            msg = "unsupported recovery descriptor"
            raise InvalidHandleError(msg)
        identities = [
            (handle.reference.channel_id, handle.reference.direction)
            for handle in self.handles
        ]
        if len(set(identities)) != len(identities):
            msg = "recovery descriptor repeats a handle"
            raise InvalidHandleError(msg)

    def fields(self) -> dict[str, JSONValue]:
        return {
            "format_version": self.format_version,
            "handles": [handle.fields() for handle in self.handles],
        }

    @classmethod
    def from_fields(cls, value: object) -> RecoveryDescriptor:
        fields = _fields(value, {"format_version", "handles"})
        handles = fields["handles"]
        if not isinstance(handles, list):
            msg = "recovery handles must be a list"
            raise InvalidHandleError(msg)
        return cls(
            tuple(
                HandleRecovery.from_fields(handle)
                for handle in cast("list[object]", handles)
            ),
            cast("Literal[1]", fields["format_version"]),
        )


def _fields(value: object, keys: set[str]) -> dict[str, object]:
    if not isinstance(value, dict) or set(cast("dict[object, object]", value)) != keys:
        msg = "invalid handle descriptor fields"
        raise InvalidHandleError(msg)
    return cast("dict[str, object]", value)
