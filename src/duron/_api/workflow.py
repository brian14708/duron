"""The ``@duron.workflow`` decorator and the `Workflow` definition."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any, Concatenate, Generic, overload
from typing_extensions import ParamSpec, TypeVar, override

from duron._api.context import WorkflowContext
from duron._api.ports import NoPorts, PortSet
from duron._api.reference import WorkflowRef
from duron._execution.validation import checked
from duron._introspection import effect_signature
from duron._ref import ref_of
from duron.codec import DefaultCodec
from duron.errors import WorkflowDefinitionError

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from duron._api.ports import _Port
    from duron.codec import Codec


_T_co = TypeVar("_T_co", covariant=True, default=object)
_P = ParamSpec("_P", default=...)
_Ports_co = TypeVar("_Ports_co", bound=PortSet, covariant=True, default=NoPorts)


class Workflow(Generic[_P, _T_co, _Ports_co]):
    """A durable orchestration definition produced by `workflow`."""

    __slots__ = ("_fn", "_ref")

    def __init__(
        self,
        fn: Callable[Concatenate[WorkflowContext, _P], Coroutine[Any, Any, _T_co]],
        ref: WorkflowRef[_P, _T_co, _Ports_co],
    ) -> None:
        self._fn = fn
        self._ref = ref

    @property
    def ref(self) -> WorkflowRef[_P, _T_co, _Ports_co]:
        return self._ref

    @property
    def name(self) -> str:
        """Stable name recorded in run history."""
        return self._ref.name

    @property
    def codec(self) -> Codec:
        """Codec selected when the workflow was defined."""
        return self._ref.codec

    @property
    def ports(self) -> tuple[_Port, ...]:
        """The workflow's host-facing I/O declarations, in declaration order."""
        return self._ref.ports

    @override
    def __repr__(self) -> str:
        return f"Workflow(name={self.name!r})"


@overload
def workflow(
    fn: Callable[Concatenate[WorkflowContext, _P], Coroutine[Any, Any, _T_co]], /
) -> Workflow[_P, _T_co]: ...
@overload
def workflow(
    *, name: str | None = None, codec: Codec | None = None, ports: type[_Ports_co]
) -> Callable[
    [Callable[Concatenate[WorkflowContext, _P], Coroutine[Any, Any, _T_co]]],
    Workflow[_P, _T_co, _Ports_co],
]: ...
@overload
def workflow(
    *, name: str | None = None, codec: Codec | None = None, ports: None = None
) -> Callable[
    [Callable[Concatenate[WorkflowContext, _P], Coroutine[Any, Any, _T_co]]],
    Workflow[_P, _T_co, NoPorts],
]: ...
def workflow(
    fn: Callable[Concatenate[WorkflowContext, _P], Coroutine[Any, Any, _T_co]]
    | None = None,
    /,
    *,
    name: str | None = None,
    codec: Codec | None = None,
    ports: type[PortSet] | None = None,
) -> (
    Workflow[..., _T_co, PortSet]
    | Callable[
        [Callable[Concatenate[WorkflowContext, _P], Coroutine[Any, Any, _T_co]]],
        Workflow[_P, _T_co, PortSet],
    ]
):
    """Mark an async function as a durable workflow.

    Annotate the first positional parameter as `WorkflowContext`. Decoration
    checks its annotation and rejects variadic parameters.
    ``name`` defaults to ``"{module}:{qualname}"``. The codec is finite JSON by
    default. Select a custom codec for application values when needed.
    Declare I/O with a ``PortSet`` and bind endpoints through ``ctx.port`` or
    ``run.port``.
    Unused ports are allowed for conditional paths.

    Example:
        ```python
        @duron.workflow(name="orders.fulfill")
        async def fulfill_order(
            ctx: duron.WorkflowContext, order_id: str
        ) -> Receipt: ...
        ```

    Returns:
        A `Workflow` definition, or a decorator producing one.

    """

    def decorate(
        user_fn: Callable[Concatenate[WorkflowContext, _P], Coroutine[Any, Any, _T_co]],
    ) -> Workflow[_P, _T_co, PortSet]:
        selected_codec = codec if codec is not None else DefaultCodec
        return _define_workflow(
            user_fn, name=name, codec=selected_codec, ports=ports or NoPorts
        )

    if fn is not None:
        return decorate(fn)
    return decorate


def _define_workflow(
    user_fn: Callable[Concatenate[WorkflowContext, _P], Coroutine[Any, Any, _T_co]],
    *,
    name: str | None,
    codec: Codec,
    ports: type[_Ports_co],
) -> Workflow[_P, _T_co, _Ports_co]:
    codec = checked(codec)
    if not inspect.iscoroutinefunction(user_fn):
        msg = "A workflow function must be defined with 'async def'"
        raise WorkflowDefinitionError(msg)
    signature = effect_signature(
        user_fn, context_type=WorkflowContext, context_name="workflow"
    )
    workflow_reference: WorkflowRef[_P, _T_co, _Ports_co] = WorkflowRef(
        ref_of(user_fn, name=name)["name"], signature, codec, ports
    )
    return Workflow(user_fn, workflow_reference)
