"""Immutable port declarations and typed execution capabilities."""

# pyright: reportPrivateUsage=false
# ruff: file-ignore[private-member-access]

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, ClassVar, Generic, TypeAlias, cast, get_args
from typing_extensions import Protocol, TypeVar, override

from duron._execution.validation import validate_schema
from duron.errors import WorkflowDefinitionError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Collection
    from contextlib import AbstractAsyncContextManager

    from duron._api.run import OutputCursor
    from duron._hints import TypeHint

_T = TypeVar("_T")
_T_co = TypeVar("_T_co", covariant=True)
_T_contra = TypeVar("_T_contra", contravariant=True)

__all__ = ("_check_port", "_declared_ports", "_stream_name_for_port", "_utc_from_us")


class _NamedPort:
    kind: str = "port"
    __orig_class__: object
    __slots__ = ("__orig_class__", "_name")

    @override
    def __setattr__(self, name: str, value: object) -> None:
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
        name = getattr(self, "_name", None)
        if name is None:
            msg = "port must be declared on a PortSet"
            raise WorkflowDefinitionError(msg)
        return cast("str", name)

    @override
    def __repr__(self) -> str:
        return f"{type(self).__name__}({getattr(self, '_name', None)!r})"

    def _bind(self, name: str) -> _NamedPort:
        """Create an immutable named copy of this declaration.

        Returns:
            A bound declaration with the supplied public name.

        """
        bound = object.__new__(type(self))
        if hasattr(self, "__orig_class__"):
            bound.__orig_class__ = self.__orig_class__
        bound._name = name
        return bound


class _ItemPort(_NamedPort, Generic[_T]):
    __slots__ = ()

    @property
    def item_type(self) -> TypeHint[_T]:
        return _declared_type(self, 0)


class Input(_ItemPort[_T]):
    """An input queue consumed by its workflow or effect."""

    kind = "input"
    __slots__ = ()

    def __init__(self) -> None:
        pass


class Output(_ItemPort[_T]):
    """An independently readable durable event log."""

    kind = "output"
    __slots__ = ()

    def __init__(self) -> None:
        pass


_Port: TypeAlias = Input[Any] | Output[Any]


class _PortSetMeta(type):
    @override
    def __setattr__(cls, name: str, value: object) -> None:
        if "__ports__" in vars(cls) and (
            name == "__ports__"
            or isinstance(value, _NamedPort)
            or any(port.name == name for port in cast("type[PortSet]", cls).__ports__)
        ):
            msg = "port sets are immutable after declaration"
            raise WorkflowDefinitionError(msg)
        super().__setattr__(name, value)

    @override
    def __delattr__(cls, name: str) -> None:
        if name == "__ports__" or any(
            port.name == name for port in cast("type[PortSet]", cls).__ports__
        ):
            msg = "port sets are immutable after declaration"
            raise WorkflowDefinitionError(msg)
        super().__delattr__(name)


class PortSet(metaclass=_PortSetMeta):
    """Declare named ports once as class attributes, without instantiation."""

    __ports__: ClassVar[tuple[_Port, ...]] = ()

    @override
    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        ports: dict[str, _Port] = {}
        for base in cls.__bases__:
            if not issubclass(base, PortSet):
                continue
            for port in base.__ports__:
                if port.name in ports and ports[port.name] is not port:
                    msg = f"ambiguous inherited port {port.name!r}"
                    raise WorkflowDefinitionError(msg)
                ports[port.name] = port
        for name, value in vars(cls).items():
            if name in ports:
                msg = f"cannot override inherited port {name!r}"
                raise WorkflowDefinitionError(msg)
            if not isinstance(value, (Input, Output)):
                continue
            value = cast("_Port", value)
            if name.startswith("_"):
                msg = "ports need public names"
                raise WorkflowDefinitionError(msg)
            bound = cast("_Port", value._bind(name))
            setattr(cls, name, bound)
            validate_schema(bound.item_type)
            ports[name] = bound
        cls.__ports__ = tuple(ports.values())


class NoPorts(PortSet):
    """The port contract for ordinary argument/result workflows."""


def _declared_ports(port_set: type[PortSet] | None) -> tuple[_Port, ...]:
    if port_set is None:
        return ()
    candidate = cast("object", port_set)
    if not isinstance(candidate, type) or not issubclass(candidate, PortSet):
        msg = "ports must be a PortSet subclass"
        raise WorkflowDefinitionError(msg)
    return port_set.__ports__


class _InputOwner(Protocol):
    async def _receive(self, port: Input[_T], /) -> _T: ...

    async def _receive_many(
        self, port: Input[_T], /, *, max_items: int | None = None, wait: bool = True
    ) -> list[_T]: ...

    def _interruptible(self, port: Input[_T]) -> AbstractAsyncContextManager[None]: ...


class _OutputOwner(Protocol):
    async def _emit(
        self, port: Output[_T], value: _T, /, *, idempotency_key: str | None = None
    ) -> None: ...


@dataclass(frozen=True, slots=True, eq=False)
class Receiver(Generic[_T_co]):
    """A workflow input capability, also selectable without consuming losers."""

    _owner: _InputOwner
    _port: Input[_T_co]

    async def receive(self) -> _T_co:
        return await self._owner._receive(self._port)

    async def receive_many(
        self, *, max_items: int | None = None, wait: bool = True
    ) -> list[_T_co]:
        return await self._owner._receive_many(
            self._port, max_items=max_items, wait=wait
        )

    def interruptible(self) -> AbstractAsyncContextManager[None]:
        return self._owner._interruptible(self._port)


class EffectReceiver(Protocol[_T_co]):
    """Effect input replayed from the beginning on each invocation."""

    async def receive(self) -> _T_co: ...


class Sender(Protocol[_T_contra]):
    async def send(
        self, value: _T_contra, /, *, idempotency_key: str | None = None
    ) -> None: ...
    async def close(self) -> None: ...


@dataclass(frozen=True, slots=True, eq=False)
class Emitter(Generic[_T]):
    _owner: _OutputOwner
    _port: Output[_T]

    async def emit(self, value: _T) -> None:
        await self._owner._emit(self._port, value)


@dataclass(frozen=True, slots=True, eq=False)
class EffectEmitter(Generic[_T]):
    """An effect output supporting explicit semantic retry keys."""

    _owner: _OutputOwner
    _port: Output[_T]

    async def emit(self, value: _T, *, idempotency_key: str | None = None) -> None:
        await self._owner._emit(self._port, value, idempotency_key=idempotency_key)


class PortReader(Protocol[_T_co]):
    """An output iterator with a resumable durable cursor."""

    @property
    def cursor(self) -> OutputCursor: ...
    def __aiter__(self) -> AsyncIterator[_T_co]: ...
    async def __anext__(self) -> _T_co: ...


class Reader(Protocol[_T_co]):
    def reader(
        self, *, after: OutputCursor | None = None, prefetch: int = 64
    ) -> PortReader[_T_co]: ...
    def __aiter__(self) -> AsyncIterator[_T_co]: ...


def _stream_name_for_port(port: _NamedPort) -> str:
    return f"__{port.kind}__:{port.name}"


def _check_port(port: _Port, declared: Collection[_Port], workflow_name: str) -> None:
    if port not in declared:
        msg = f"{workflow_name!r} used undeclared port {port!r}"
        raise WorkflowDefinitionError(msg)


def _utc_from_us(ts_us: int) -> datetime:
    return datetime.fromtimestamp(ts_us / 1_000_000, tz=UTC)


def _declared_type(obj: _NamedPort, index: int) -> TypeHint[Any]:
    args: tuple[TypeHint[Any], ...] = get_args(getattr(obj, "__orig_class__", None))
    if len(args) > index:
        return args[index]
    msg = f"port {obj.name!r} requires a subscripted payload type"
    raise TypeError(msg)
