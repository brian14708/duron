"""Typed, named ports connecting workflow code and host code.

Ports are module-level immutable declarations with stable names. The same
declaration object is used by workflow code (through the context) and by host
code (through the run handle), so external names are never duplicated.

Declare the payload type either by subscripting at the call site or by passing
it explicitly::

    events = Output[str]("events")  # subscripted
    events = Output("events", str)  # explicit

Both record ``str`` as the payload type. A bare ``Output("events")`` — including
the ``events: Output[str] = Output("events")`` annotation form, where the
subscript is only an annotation — leaves the type unspecified, so schema-aware
codecs fall back to structural encoding. Prefer one of the two typed forms.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Generic, cast, get_args
from typing_extensions import Any, TypeVar, override

from duron._api.effect import validate_error_types
from duron.typing import UnspecifiedType

if TYPE_CHECKING:
    from duron.codec import Codec
    from duron.typing import JSONValue, TypeHint

_T = TypeVar("_T")
_Req = TypeVar("_Req")
_Res = TypeVar("_Res")


class _NamedPort:
    """A named port declaration; ``kind`` determines its durable stream name."""

    kind: str = "port"

    def __init__(self, name: str) -> None:
        self.name = name

    @property
    def stream_name(self) -> str:
        """The durable log stream name backing this port."""
        return f"__{self.kind}__:{self.name}"

    @override
    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.name!r})"


class _Port(_NamedPort, Generic[_T]):
    def __init__(self, name: str, item_type: TypeHint[_T] = UnspecifiedType) -> None:
        super().__init__(name)
        self._item_type = item_type

    @property
    def item_type(self) -> TypeHint[_T]:
        """The declared payload type, or unspecified if none was declared."""
        return _declared_type(self._item_type, self, 0)


class Input(_Port[_T]):
    """A host-to-workflow queue delivering values FIFO, consumed once each."""

    kind = "input"


class Output(_Port[_T]):
    """A durable workflow-to-host event log read with client-supplied cursors."""

    kind = "output"


class Signal(_Port[_T]):
    """A host-to-workflow notification that may interrupt an awaited operation."""

    kind = "signal"


class Request(_NamedPort, Generic[_Req, _Res]):
    """A durable workflow-to-host interaction expecting exactly one reply.

    Declare the payload types by subscripting (``Request[str, bool]("name")``)
    or explicitly (``Request("name", request_type=str, response_type=bool)``).
    """

    kind = "request"

    def __init__(
        self,
        name: str,
        request_type: TypeHint[_Req] = UnspecifiedType,
        response_type: TypeHint[_Res] = UnspecifiedType,
        *,
        raises: tuple[type[BaseException], ...] = (),
    ) -> None:
        super().__init__(name)
        self._request_type = request_type
        self._response_type = response_type
        self._raises = validate_error_types(raises, f"Request({name!r})")

    @property
    def request_type(self) -> TypeHint[_Req]:
        return _declared_type(self._request_type, self, 0)

    @property
    def response_type(self) -> TypeHint[_Res]:
        return _declared_type(self._response_type, self, 1)

    @property
    def raises(self) -> tuple[type[BaseException], ...]:
        """Exception types host code may fail this request with.

        Declared types (and built-in exceptions) round-trip as themselves when
        a recorded failure is replayed; undeclared types are re-raised as
        :class:`~duron.RemoteEffectError`.
        """
        return self._raises


def utc_from_us(ts_us: int) -> datetime:
    """Convert a durable-log microsecond timestamp to an aware UTC datetime.

    Log timestamps are integer microseconds (``BaseEntry``'s ``ts``), while the
    deterministic clock's :meth:`~duron.WorkflowContext.now` counterpart is
    seconds. Keeping the microsecond-to-second scaling here means every consumer
    of log time applies one convention rather than each re-deriving it.

    Returns:
        The corresponding timezone-aware UTC datetime.

    """
    return datetime.fromtimestamp(ts_us / 1_000_000, tz=timezone.utc)


def _type_args(obj: object) -> tuple[TypeHint[object], ...]:
    orig = getattr(obj, "__orig_class__", None)
    if orig is None:
        return ()
    return tuple(get_args(orig))


def _declared_type(explicit: TypeHint[Any], obj: object, index: int) -> TypeHint[Any]:
    """Return the explicitly declared type, else the subscript arg at ``index``.

    Returns:
        The declared payload type, or unspecified if none was declared.

    """
    if explicit is not UnspecifiedType:
        return explicit
    args = _type_args(obj)
    return args[index] if len(args) > index else UnspecifiedType


def encode_request_envelope(
    codec: Codec, request_id: str, value: object, request_type: TypeHint[Any]
) -> list[object]:
    """Encode a request payload with its id into the durable wire envelope.

    The envelope is a ``[request_id, encoded_value]`` pair persisted as one
    stream entry on the request port. :func:`decode_request_envelope` is the
    only reader, so the framing cannot drift between the workflow and host
    sides.

    Returns:
        The envelope to append to the request port's stream.

    """
    return [request_id, codec.encode_json(value, request_type)]


def decode_request_envelope(
    codec: Codec, raw: JSONValue
) -> tuple[str, JSONValue] | None:
    """Decode an envelope written by :func:`encode_request_envelope`.

    The envelope was persisted through the workflow codec, so it is decoded
    back to a list before framing (opaque codecs such as PickleCodec store it
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
