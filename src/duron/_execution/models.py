"""Immutable effect identity, invocation context, and execution policies."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generic, Literal, cast, overload
from typing_extensions import TypeVar

from duron._execution.validation import validate_schema
from duron.errors import DuronError, InvalidHandleError, WorkflowDefinitionError

if TYPE_CHECKING:
    from collections.abc import Mapping

    from duron._api.ports import EffectEmitter, EffectReceiver, Input, Output, _Port
    from duron._execution.handles import HandleResolver
    from duron._hints import JSONValue, TypeHint
    from duron._history.entries import InvocationIdentityData

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


def _empty_port_bindings() -> dict[_Port, object]:
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
    return hashlib.sha256(
        domain.encode("ascii") + b"\0" + canonical_json(value)
    ).hexdigest()


@dataclass(frozen=True, slots=True)
class EffectId:
    """Opaque stable identifier returned by an executor registration."""

    value: str


@dataclass(frozen=True, slots=True)
class EffectIdentity:
    """Versioned public identity of an effect definition."""

    name: str
    version: str
    format_version: Literal[0] = 0
    _effect_id: EffectId = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        name = cast("object", self.name)
        version = cast("object", self.version)
        if (
            not isinstance(name, str)
            or not isinstance(version, str)
            or not name
            or not version
        ):
            msg = "effect name and version must be nonempty strings"
            raise WorkflowDefinitionError(msg)
        object.__setattr__(
            self,
            "_effect_id",
            EffectId(digest("duron.effect.v0", [self.name, self.version])),
        )

    @property
    def effect_id(self) -> EffectId:
        return self._effect_id


@dataclass(frozen=True, slots=True)
class ExecutionIdentity:
    """One durable effect call, stable across crash recovery."""

    run_seed: str
    operation_id: str
    effect_id: EffectId
    input_digest: str
    format_version: Literal[0] = 0


@dataclass(frozen=True, slots=True)
class InvocationIdentity:
    """Durable counters for a physical invocation of an effect call."""

    invocation_number: int
    recovery_count: int
    format_version: Literal[0] = 0

    def __post_init__(self) -> None:
        validate_counter(self.invocation_number, "invocation_number", minimum=1)
        validate_counter(self.recovery_count, "recovery_count", minimum=0)
        if self.invocation_number != 1 + self.recovery_count:
            msg = "invocation counters are inconsistent"
            raise WorkflowDefinitionError(msg)

    def fields(self) -> InvocationIdentityData:
        return {
            "format_version": self.format_version,
            "invocation_number": self.invocation_number,
            "recovery_count": self.recovery_count,
        }


@dataclass(frozen=True, slots=True)
class EffectContextImpl:
    """Identity and recovery information supplied to an effect body.

    Effects return or raise; completion authority is owned by the runtime.
    """

    identity: ExecutionIdentity
    effect: EffectIdentity
    invocation: InvocationIdentity
    recovering: bool = False
    handles: HandleResolver | None = field(default=None, repr=False, compare=False)
    _port_bindings: Mapping[_Port, object] = field(
        default_factory=_empty_port_bindings, repr=False, compare=False
    )

    @overload
    def port(self, declaration: Input[_T], /) -> EffectReceiver[_T]: ...
    @overload
    def port(self, declaration: Output[_T], /) -> EffectEmitter[_T]: ...
    def port(self, declaration: _Port, /) -> EffectReceiver[Any] | EffectEmitter[Any]:
        try:
            endpoint = self._port_bindings[declaration]
        except KeyError:
            msg = f"effect port {declaration!r} is not bound to this invocation"
            raise WorkflowDefinitionError(msg) from None
        return cast("EffectReceiver[Any] | EffectEmitter[Any]", endpoint)

    @property
    def invocation_number(self) -> int:
        return self.invocation.invocation_number

    @property
    def recovery_count(self) -> int:
        return self.invocation.recovery_count


def validate_counter(value: int, name: str, *, minimum: int) -> None:
    if type(value) is not int or value < minimum:
        msg = f"{name} must be an integer >= {minimum}"
        raise WorkflowDefinitionError(msg)


def validate_error_types(
    types: tuple[type[BaseException], ...], owner: str
) -> tuple[type[BaseException], ...]:
    for t in cast("tuple[object, ...]", types):
        if not (isinstance(t, type) and issubclass(t, Exception)):
            msg = f"{owner} declares an unsupported exception type: {t!r}"
            raise WorkflowDefinitionError(msg)
        if (
            issubclass(t, DuronError)
            and t.__dict__.get("__duron_round_trip__") is not True
        ):
            msg = f"{owner} declares a runtime error without round-trip support: {t!r}"
            raise WorkflowDefinitionError(msg)
    return tuple(types)


@dataclass(frozen=True, slots=True)
class EffectOperation:
    """Resolved, snapshotted call handed from the public API to the session."""

    identity: EffectIdentity
    seed: str
    input_digest: str
    descriptor: bytes
    raises: tuple[type[BaseException], ...]
    inputs: bytes
    handles: tuple[HandleReference, ...]
