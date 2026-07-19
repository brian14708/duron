"""The ``@duron.workflow`` decorator and the `Workflow` definition."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import functools
import inspect
from collections.abc import Callable, Coroutine
from typing import (
    TYPE_CHECKING,
    Any,
    Concatenate,
    Generic,
    cast,
    final,
    get_origin,
    overload,
)
from typing_extensions import ParamSpec, TypeVar, override

from duron._api.context import WorkflowContext
from duron._api.effect import _default_name
from duron._api.ports import NoPorts, PortSet, _declared_ports
from duron._api.reference import WorkflowRef, _codec_identity
from duron._execution.validation import checked
from duron._hints import UnspecifiedType
from duron._introspection import effect_signature, inspect_function
from duron._runtime.api_context import WorkflowContextImpl
from duron.codec import DefaultCodec
from duron.errors import WorkflowDefinitionError

if TYPE_CHECKING:
    from duron._api.ports import _Port
    from duron._hints import TypeHint
    from duron._runtime.context import Context
    from duron.codec import Codec


_T = TypeVar("_T", default=Any)
_P = ParamSpec("_P", default=...)
_Ports = TypeVar("_Ports", bound=PortSet, default=NoPorts)


@final
class _DurableFn(Generic[_P, _T]):
    __slots__ = ("codec", "fn")

    def __init__(
        self,
        codec: Codec,
        fn: Callable[Concatenate[Context, _P], Coroutine[Any, Any, _T]],
    ) -> None:
        self.codec = codec
        self.fn = fn

    def positional_type(self, index: int) -> TypeHint[Any]:
        """Return the declared type of the ``index``-th user positional argument.

        The runtime injects the leading context parameter, so user-supplied
        positional argument ``i`` maps to declared parameter ``i + 1``.

        Returns:
            The declared type hint, or unspecified when out of range.

        """
        hint = inspect_function(self.fn)
        parameters = hint.parameters
        if index + 1 < len(parameters):
            return hint.parameter_types.get(parameters[index + 1], UnspecifiedType)
        return UnspecifiedType


class Workflow(Generic[_P, _T, _Ports]):
    """A durable orchestration definition produced by `workflow`."""

    __slots__ = (
        "_codec",
        "_durable",
        "_name",
        "_ports",
        "_ref",
        "_signature",
        "_version",
    )

    def __init__(
        self,
        durable: _DurableFn[_P, _T],
        *,
        name: str,
        version: str,
        codec: Codec,
        ports: tuple[_Port, ...],
        port_set: type[_Ports],
        codec_id: str | None,
    ) -> None:
        self._durable = durable
        signature = inspect.signature(durable.fn)
        self._signature = signature.replace(
            parameters=tuple(signature.parameters.values())[1:]
        )
        self._name = name
        self._version = version
        self._codec = codec
        self._ports = ports
        self._ref: WorkflowRef[_P, _T, _Ports] = WorkflowRef(
            name,
            version,
            self._signature,
            codec,
            _codec_identity(codec, codec_id),
            port_set,
        )

    @property
    def ref(self) -> WorkflowRef[_P, _T, _Ports]:
        return self._ref

    @property
    def name(self) -> str:
        """Stable name recorded in run history."""
        return self._name

    @property
    def version(self) -> str:
        """Definition version recorded in run history."""
        return self._version

    @property
    def codec(self) -> Codec:
        """Codec selected when the workflow was defined."""
        return self._codec

    @property
    def ports(self) -> tuple[_Port, ...]:
        """The workflow's host-facing I/O declarations, in declaration order."""
        return self._ports

    @override
    def __repr__(self) -> str:
        return f"Workflow(name={self.name!r}, version={self.version!r})"


_WorkflowFn = Callable[..., Coroutine[Any, Any, _T]]


@overload
def workflow(
    fn: Callable[Concatenate[WorkflowContext, _P], Coroutine[Any, Any, _T]], /
) -> Workflow[_P, _T]: ...
@overload
def workflow(
    *,
    name: str | None = None,
    version: str = "0",
    codec: Codec | None = None,
    codec_id: str | None = None,
    ports: type[_Ports],
) -> Callable[
    [Callable[Concatenate[WorkflowContext, _P], Coroutine[Any, Any, _T]]],
    Workflow[_P, _T, _Ports],
]: ...
@overload
def workflow(
    *,
    name: str | None = None,
    version: str = "0",
    codec: Codec | None = None,
    codec_id: str | None = None,
    ports: None = None,
) -> Callable[
    [Callable[Concatenate[WorkflowContext, _P], Coroutine[Any, Any, _T]]],
    Workflow[_P, _T, NoPorts],
]: ...
def workflow(
    fn: _WorkflowFn[_T] | None = None,
    /,
    *,
    name: str | None = None,
    version: str = "0",
    codec: Codec | None = None,
    codec_id: str | None = None,
    ports: type[PortSet] | None = None,
) -> (
    Workflow[_P, _T, Any]
    | Callable[
        [Callable[Concatenate[WorkflowContext, _P], Coroutine[Any, Any, _T]]],
        Workflow[_P, _T, Any],
    ]
):
    """Mark an async function as a durable workflow.

    Annotate the first positional parameter as `WorkflowContext`. Decoration
    checks its annotation and rejects variadic parameters.
    ``name`` defaults to ``"{module}:{qualname}"``. The codec is finite JSON by
    default. Select a custom codec for application values when needed, and
    bump ``version`` when its encoding contract changes incompatibly.
    ``version`` is the persisted public identity. Declare I/O with a
    ``PortSet`` and bind endpoints through ``ctx.port`` or ``run.port``.
    Unused ports are allowed for conditional paths.

    Example:
        ```python
        @duron.workflow(name="orders.fulfill", version="3")
        async def fulfill_order(
            ctx: duron.WorkflowContext, order_id: str
        ) -> Receipt: ...
        ```

    Returns:
        A `Workflow` definition, or a decorator producing one.

    """

    def decorate(
        user_fn: Callable[Concatenate[WorkflowContext, _P], Coroutine[Any, Any, _T]],
    ) -> Workflow[_P, _T, Any]:
        selected_codec = codec if codec is not None else DefaultCodec
        return _define_workflow(
            user_fn,
            name=name,
            version=version,
            codec=selected_codec,
            codec_id=codec_id,
            ports=ports,
        )

    if fn is not None:
        return decorate(fn)
    return decorate


def _define_workflow(
    user_fn: Callable[Concatenate[WorkflowContext, _P], Coroutine[Any, Any, _T]],
    *,
    name: str | None,
    version: str,
    codec: Codec,
    codec_id: str | None,
    ports: type[PortSet] | None,
) -> Workflow[_P, _T, Any]:
    codec = checked(codec)
    if not inspect.iscoroutinefunction(user_fn):
        msg = "A workflow function must be defined with 'async def'"
        raise WorkflowDefinitionError(msg)
    _validate_context_param(user_fn)
    signature = effect_signature(user_fn, context_type=WorkflowContext)
    declarations = _declared_ports(ports)
    resolved_name = name or _default_name(user_fn)

    @functools.wraps(user_fn)
    async def shim(ctx: Context, *args: object, **kwargs: object) -> _T:
        wc: WorkflowContext = WorkflowContextImpl(
            ctx, codec, workflow_name=resolved_name, ports=declarations
        )
        call = cast("Callable[..., Coroutine[Any, Any, _T]]", user_fn)
        return await call(wc, *args, **kwargs)

    context_parameter = next(
        iter(inspect.signature(user_fn).parameters.values())
    ).replace(annotation=WorkflowContext)
    cast("Any", shim).__signature__ = signature.replace(
        parameters=[context_parameter, *signature.parameters.values()]
    )
    durable_fn: _DurableFn[_P, _T] = _DurableFn(codec=codec, fn=cast("Any", shim))
    return Workflow(
        durable_fn,
        name=resolved_name,
        version=version,
        codec=codec,
        ports=declarations,
        port_set=ports or NoPorts,
        codec_id=codec_id,
    )


def _validate_context_param(fn: Callable[..., object]) -> None:
    """Reject a workflow whose first parameter is not a ``WorkflowContext``.

    A malformed signature fails here with a clear error instead of an opaque
    `TypeError` when the run later invokes the function.

    Raises:
        WorkflowDefinitionError: if the first
            parameter is missing or not annotated as ``WorkflowContext``.

    """
    try:
        signature = inspect.signature(fn, eval_str=True)
    except (NameError, TypeError):
        signature = inspect.signature(fn)
    parameters = list(signature.parameters.values())
    if not parameters:
        msg = "A workflow function must take a WorkflowContext as its first parameter"
        raise WorkflowDefinitionError(msg)
    first = parameters[0]
    annotation = first.annotation
    # Accept the resolved class or a string annotation whose final component is
    # ``WorkflowContext`` (handles ``from __future__ import annotations``).
    valid = (
        annotation is WorkflowContext
        or get_origin(annotation) is WorkflowContext
        or (
            isinstance(annotation, str)
            and annotation.split("[", maxsplit=1)[0].rsplit(".", maxsplit=1)[-1]
            == "WorkflowContext"
        )
    )
    if (
        first.kind
        not in {
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        }
        or not valid
    ):
        msg = "The first workflow parameter must be a positional WorkflowContext"
        raise WorkflowDefinitionError(msg)
    # Variadic parameters cannot be typed or encoded for durable replay, so
    # reject them anywhere in the signature (not just the first position).
    for parameter in parameters:
        if parameter.kind in {
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        }:
            msg = f"Variadic parameter {parameter.name!r} is not supported"
            raise WorkflowDefinitionError(msg)
