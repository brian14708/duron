"""Executable-free, typed workflow contracts."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import inspect
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Concatenate, Generic, cast, overload
from typing_extensions import ParamSpec, TypeVar

from duron._api.context import WorkflowContext
from duron._api.ports import NoPorts, PortSet, _declared_ports
from duron._execution.models import canonical_json
from duron._execution.registration import schema_id
from duron._execution.validation import CheckedCodec, checked
from duron._introspection import effect_signature
from duron.codec import DefaultCodec
from duron.errors import WorkflowDefinitionError

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from duron._api.ports import _Port
    from duron._hints import JSONValue, TypeHint
    from duron.codec import Codec

_P = ParamSpec("_P")
_T = TypeVar("_T")
_Ports = TypeVar("_Ports", bound=PortSet, default=NoPorts)
_DeclaredPorts = TypeVar("_DeclaredPorts", bound=PortSet)


@dataclass(frozen=True, slots=True)
class WorkflowRef(Generic[_P, _T, _Ports]):
    """A contract without an executable function or an import instruction.

    Use ``workflow.ref`` or decorate a shared contract with ``workflow_ref``.
    ``codec_id`` identifies the codec's wire contract, including configuration.
    """

    name: str
    version: str
    signature: inspect.Signature
    codec: Codec
    codec_id: str
    port_set: type[_Ports]

    def __post_init__(self) -> None:
        object.__setattr__(self, "codec", checked(self.codec))
        if not self.name or not self.version or not self.codec_id:
            msg = "workflow name, version, and codec identity must be nonempty"
            raise WorkflowDefinitionError(msg)
        for parameter in self.signature.parameters.values():
            if parameter.kind in {parameter.VAR_POSITIONAL, parameter.VAR_KEYWORD}:
                msg = "variadic workflow parameters are not supported"
                raise WorkflowDefinitionError(msg)
        _ = self._identity

    @property
    def ports(self) -> tuple[_Port, ...]:
        return _declared_ports(self.port_set)

    @property
    def result_type(self) -> TypeHint[_T]:
        return cast("TypeHint[_T]", self.signature.return_annotation)

    @property
    def _identity(self) -> bytes:
        parameters: list[JSONValue] = []
        for parameter in self.signature.parameters.values():
            item: dict[str, JSONValue] = {
                "name": parameter.name,
                "kind": parameter.kind.name,
                "schema": schema_id(parameter.annotation),
                "required": parameter.default is inspect.Parameter.empty,
            }
            if parameter.default is not inspect.Parameter.empty:
                item["default"] = self.codec.encode_json(
                    parameter.default, parameter.annotation
                )
            parameters.append(item)
        ports: list[JSONValue] = [
            {
                "name": port.name,
                "direction": port.kind,
                "schema": schema_id(port.item_type),
            }
            for port in self.ports
        ]
        return canonical_json({
            "name": self.name,
            "version": self.version,
            "codec": self.codec_id,
            "parameters": parameters,
            "result": schema_id(self.result_type),
            "ports": ports,
        })

    def _encode(self, *args: _P.args, **kwargs: _P.kwargs) -> bytes:
        bound = self.signature.bind(*args, **kwargs)
        bound.apply_defaults()
        values: dict[str, JSONValue] = {
            name: self.codec.encode_json(
                value, self.signature.parameters[name].annotation
            )
            for name, value in bound.arguments.items()
        }
        return canonical_json(values)

    def _invocation(
        self, encoded: bytes
    ) -> tuple[list[JSONValue], dict[str, JSONValue]]:
        values = cast("dict[str, JSONValue]", json.loads(encoded))
        args: list[JSONValue] = []
        kwargs: dict[str, JSONValue] = {}
        for parameter in self.signature.parameters.values():
            if parameter.kind is inspect.Parameter.KEYWORD_ONLY:
                kwargs[parameter.name] = values[parameter.name]
            else:
                args.append(values[parameter.name])
        return args, kwargs


def _codec_identity(codec: Codec, explicit: str | None) -> str:
    if explicit is not None:
        return explicit
    if isinstance(codec, CheckedCodec):
        codec = codec.codec
    selected = codec if isinstance(codec, type) else type(codec)
    return f"{selected.__module__}:{selected.__qualname__}"


@overload
def workflow_ref(
    *,
    name: str,
    version: str = "0",
    codec: Codec = DefaultCodec,
    codec_id: str | None = None,
    ports: type[_DeclaredPorts],
) -> Callable[
    [Callable[Concatenate[WorkflowContext, _P], Coroutine[Any, Any, _T]]],
    WorkflowRef[_P, _T, _DeclaredPorts],
]: ...
@overload
def workflow_ref(
    *,
    name: str,
    version: str = "0",
    codec: Codec = DefaultCodec,
    codec_id: str | None = None,
) -> Callable[
    [Callable[Concatenate[WorkflowContext, _P], Coroutine[Any, Any, _T]]],
    WorkflowRef[_P, _T, NoPorts],
]: ...
def workflow_ref(
    *,
    name: str,
    version: str = "0",
    codec: Codec = DefaultCodec,
    codec_id: str | None = None,
    ports: type[PortSet] = NoPorts,
) -> Callable[
    [Callable[Concatenate[WorkflowContext, _P], Coroutine[Any, Any, _T]]],
    WorkflowRef[_P, _T, Any],
]:
    """Describe a workflow using an annotated async contract stub.

    The stub body is discarded. Workers register executable definitions explicitly.

    Returns:
        A decorator producing an executable-free workflow reference.

    """

    def decorate(
        fn: Callable[Concatenate[WorkflowContext, _P], Coroutine[Any, Any, _T]],
    ) -> WorkflowRef[_P, _T, Any]:
        return WorkflowRef(
            name,
            version,
            effect_signature(fn, context_type=WorkflowContext),
            codec,
            _codec_identity(codec, codec_id),
            ports,
        )

    return decorate
