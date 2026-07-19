"""The ``@duron.workflow`` decorator and the :class:`Workflow` definition."""

from __future__ import annotations

import functools
import inspect
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Concatenate, Generic, cast
from typing_extensions import Any, ParamSpec, TypeVar, overload, override

from duron._api.context import WorkflowContext
from duron._api.effect import default_name
from duron._api.invoke import Invocation
from duron._decorator.durable import DurableFn
from duron.codec._base import DefaultCodec
from duron.errors import WorkflowDefinitionError

if TYPE_CHECKING:
    from duron._core.context import Context
    from duron.codec import Codec

_T = TypeVar("_T")
_P = ParamSpec("_P")


class Workflow(Generic[_P, _T]):
    """A durable orchestration definition produced by :func:`workflow`."""

    __slots__ = ("_durable", "_signature", "codec", "name", "version")

    def __init__(
        self, durable: DurableFn[_P, _T], *, name: str, version: str, codec: Codec
    ) -> None:
        self._durable = durable
        signature = inspect.signature(durable.fn)
        self._signature = signature.replace(
            parameters=tuple(signature.parameters.values())[1:]
        )
        self.name = name
        self.version = version
        self.codec = codec

    @property
    def durable(self) -> DurableFn[_P, _T]:
        """The internal durable-function carrier consumed by the runtime."""
        return self._durable

    def __call__(self, /, *args: _P.args, **kwargs: _P.kwargs) -> Invocation[_T]:
        """Bind arguments, producing a wireable :class:`Invocation`.

        The workflow is not started here; wire ports on the returned
        invocation and pass it to :func:`duron.run`.

        Returns:
            An unstarted invocation carrying these arguments.

        """
        bound = self._signature.bind(*args, **kwargs)
        bound.apply_defaults()
        return Invocation(self, tuple(bound.args), dict(bound.kwargs))

    @override
    def __repr__(self) -> str:
        return f"Workflow(name={self.name!r}, version={self.version!r})"


_WorkflowFn = Callable[Concatenate[WorkflowContext, _P], Coroutine[Any, Any, _T]]


@overload
def workflow(fn: _WorkflowFn[_P, _T], /) -> Workflow[_P, _T]: ...
@overload
def workflow(
    *, name: str | None = None, version: str = "1", codec: Codec | None = None
) -> Callable[[_WorkflowFn[_P, _T]], Workflow[_P, _T]]: ...
def workflow(
    fn: _WorkflowFn[_P, _T] | None = None,
    /,
    *,
    name: str | None = None,
    version: str = "1",
    codec: Codec | None = None,
) -> Workflow[_P, _T] | Callable[[_WorkflowFn[_P, _T]], Workflow[_P, _T]]:
    """Mark an async function as a durable workflow.

    The first parameter must be a :class:`~duron.WorkflowContext` (enforced by
    the type checker). ``name`` defaults to ``"{module}:{qualname}"``;
    ``version`` is the persisted public identity. Effects and ports are
    referenced from the body, not injected.

    Example:
        ```python
        @duron.workflow(name="orders.fulfill", version="3")
        async def fulfill_order(
            ctx: duron.WorkflowContext, order_id: str
        ) -> Receipt: ...
        ```

    Returns:
        A :class:`Workflow` definition, or a decorator producing one.

    """

    def decorate(user_fn: _WorkflowFn[_P, _T]) -> Workflow[_P, _T]:
        if not inspect.iscoroutinefunction(user_fn):
            msg = "A workflow function must be defined with 'async def'"
            raise WorkflowDefinitionError(msg)
        _validate_context_param(user_fn)
        resolved_codec = codec or DefaultCodec
        resolved_name = name or default_name(user_fn)

        @functools.wraps(user_fn)
        async def shim(ctx: Context, *args: object, **kwargs: object) -> _T:
            wc = WorkflowContext(ctx, resolved_codec)
            call = cast("Callable[..., Coroutine[Any, Any, _T]]", user_fn)
            return await call(wc, *args, **kwargs)

        durable_fn: DurableFn[_P, _T] = DurableFn(
            codec=resolved_codec, fn=cast("Any", shim)
        )
        return Workflow(
            durable_fn, name=resolved_name, version=version, codec=resolved_codec
        )

    if fn is not None:
        return decorate(fn)
    return decorate


def _validate_context_param(fn: Callable[..., object]) -> None:
    """Reject a workflow whose first parameter is not a ``WorkflowContext``.

    Mirrors the decoration-time guard the old ``@durable`` provided so a
    malformed signature fails here with a clear error instead of an opaque
    ``TypeError`` when the run later invokes the function.

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
    valid = annotation is WorkflowContext or (
        isinstance(annotation, str)
        and annotation.rsplit(".", maxsplit=1)[-1] == "WorkflowContext"
    )
    if (
        first.kind in {inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD}
        or not valid
    ):
        msg = "The first workflow parameter must be annotated as WorkflowContext"
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
