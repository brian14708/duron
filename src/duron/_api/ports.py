"""Typed, named ports connecting workflow code and host code.

Ports are module-level immutable declarations with stable names. The same
declaration object is used by workflow code (through the context) and by host
code (through the run handle), so external names are never duplicated.

Declare the payload type by subscripting at the call site::

    events = Output[str]("events")

The subscript must be on the constructor call so the payload type is available
at runtime for codecs and durable schema validation.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Generic, cast, get_args
from typing_extensions import TypeVar, override

from duron._runtime.execution.effect import validate_error_types

if TYPE_CHECKING:
    from duron.codec import Codec
    from duron.types import JSONValue, TypeHint

_T = TypeVar("_T")
_Req = TypeVar("_Req")
_Res = TypeVar("_Res")


class _NamedPort:
    """A named port declaration; ``kind`` determines its durable stream name."""

    kind: str = "port"

    __slots__ = ("__orig_class__", "_name")

    def __init__(self, name: str) -> None:
        if not isinstance(cast("object", name), str) or not name:
            msg = "port name must be a non-empty string"
            raise ValueError(msg)
        self._name = name

    @override
    def __setattr__(self, name: str, value: object) -> None:
        # Generic aliases install __orig_class__ after construction. Each slot
        # may be initialized once, preserving runtime types without mutable ports.
        if hasattr(self, name):
            msg = "port declarations are immutable"
            raise AttributeError(msg)
        object.__setattr__(self, name, value)

    @override
    def __delattr__(self, name: str) -> None:
        msg = "port declarations are immutable"
        raise AttributeError(msg)

    @property
    def name(self) -> str:
        """Stable public name of this declaration."""
        return self._name

    @property
    def stream_name(self) -> str:
        """The durable log stream name backing this port."""
        return f"__{self.kind}__:{self.name}"

    @override
    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.name!r})"


class _Port(_NamedPort, Generic[_T]):
    __slots__ = ()

    @property
    def item_type(self) -> TypeHint[_T]:
        """The declared payload type."""
        return _declared_type(self, 0)


class Input(_Port[_T]):
    """A host-to-workflow queue delivering values FIFO, consumed once each."""

    kind = "input"
    __slots__ = ()


class Output(_Port[_T]):
    """A durable workflow-to-host event log read with client-supplied cursors."""

    kind = "output"
    __slots__ = ()


class Interrupt(_Port[_T]):
    """A host-to-workflow notification that may interrupt an awaited operation."""

    kind = "interrupt"
    __slots__ = ()


class Request(_NamedPort, Generic[_Req, _Res]):
    """A durable workflow-to-host interaction expecting exactly one reply.

    Declare payload types by subscripting (``Request[str, bool]("name")``).
    """

    kind = "request"

    __slots__ = ("_raises",)

    def __init__(
        self, name: str, *, raises: tuple[type[BaseException], ...] = ()
    ) -> None:
        super().__init__(name)
        self._raises = validate_error_types(raises, f"Request({name!r})")

    @property
    def request_type(self) -> TypeHint[_Req]:
        return _declared_type(self, 0)

    @property
    def response_type(self) -> TypeHint[_Res]:
        return _declared_type(self, 1)

    @property
    def raises(self) -> tuple[type[BaseException], ...]:
        """Exception types host code may fail this request with.

        Built-in exceptions such as `ValueError` need no declaration.
        `PendingRequest.fail` rejects undeclared custom types with
        `UndeclaredErrorTypeError`, leaving the request pending. Declared
        failures are reconstructed from their recorded arguments.
        """
        return self._raises


def utc_from_us(ts_us: int) -> datetime:
    """Convert a durable-log microsecond timestamp to an aware UTC datetime.

    Log timestamps are integer microseconds (`BaseEntry`'s `ts`). Convert them
    to the same timezone-aware UTC datetime type returned by
    `WorkflowContext.now`.

    Returns:
        The corresponding timezone-aware UTC datetime.

    """
    return datetime.fromtimestamp(ts_us / 1_000_000, tz=UTC)


def _type_args(obj: object) -> tuple[TypeHint[object], ...]:
    orig = getattr(obj, "__orig_class__", None)
    if orig is None:
        return ()
    return tuple(get_args(orig))


def _declared_type(obj: _NamedPort, index: int) -> TypeHint[Any]:
    """Return the payload type declared on a subscripted port constructor.

    Returns:
        The declared payload type.

    Raises:
        TypeError: If the port was not constructed with payload type arguments.

    """
    args = _type_args(obj)
    if len(args) > index:
        return args[index]
    msg = f"port {obj.name!r} requires a subscripted payload type"
    raise TypeError(msg)


def encode_request_envelope(
    codec: Codec, request_id: str, value: object, request_type: TypeHint[Any]
) -> list[object]:
    """Encode a request payload with its id into the durable wire envelope.

    The envelope is a ``[request_id, encoded_value]`` pair persisted as one
    stream entry on the request port. `decode_request_envelope` is the
    only reader, so the framing cannot drift between the workflow and host
    sides.

    Returns:
        The envelope to append to the request port's stream.

    """
    return [request_id, codec.encode_json(value, request_type)]


def decode_request_envelope(
    codec: Codec, raw: JSONValue
) -> tuple[str, JSONValue] | None:
    """Decode an envelope written by `encode_request_envelope`.

    The envelope was persisted through the workflow codec, so it is decoded
    back to a list before framing (opaque custom codecs can store it
    as a non-list scalar otherwise).

    Returns:
        The ``(request_id, encoded_value)`` pair, or ``None`` if ``raw`` is
        not envelope-shaped.

    """
    value: object = codec.decode_json(raw, list[object])
    if not isinstance(value, list):
        return None
    items = cast("list[object]", value)
    if len(items) != 2:
        return None
    request_id, encoded = items
    if not isinstance(request_id, str):
        return None
    return request_id, cast("JSONValue", encoded)
