"""The ``@duron.workflow`` decorator and the `Workflow` definition."""

from __future__ import annotations

import functools
import inspect
from collections.abc import Callable, Coroutine, Sequence
from typing import TYPE_CHECKING, Any, Concatenate, Generic, cast, overload
from typing_extensions import ParamSpec, TypeVar, override

from duron._api.context import WorkflowContext
from duron._api.run import WorkflowCall
from duron._runtime.decorator import DurableFn
from duron._runtime.execution.effect import default_name
from duron._runtime.execution.executor import resolve_codec_id
from duron._runtime.workflow.api_context import WorkflowContextImpl
from duron.codec import DefaultCodec
from duron.errors import WorkflowDefinitionError

if TYPE_CHECKING:
    from duron._api.dependencies import Dependency
    from duron._runtime.workflow.context import Context
    from duron.codec import Codec

_T = TypeVar("_T")
_P = ParamSpec("_P")


class Workflow(Generic[_P, _T]):
    """A durable orchestration definition produced by `workflow`."""

    __slots__ = (
        "_durable",
        "_requires",
        "_signature",
        "codec",
        "codec_id",
        "name",
        "version",
    )

    def __init__(
        self,
        durable: DurableFn[_P, _T],
        *,
        name: str,
        version: str,
        codec: Codec,
        codec_id: str,
        requires: tuple[Dependency, ...],
    ) -> None:
        self._durable = durable
        signature = inspect.signature(durable.fn)
        self._signature = signature.replace(
            parameters=tuple(signature.parameters.values())[1:]
        )
        self.name = name
        self.version = version
        self.codec = codec
        self.codec_id = codec_id
        self._requires = requires

    @property
    def requires(self) -> tuple[Dependency, ...]:
        """Declared effects and ports, in declaration order.

        Use these same objects in the workflow body. This is advisory metadata,
        not a service registry or a persisted part of the workflow identity.
        """
        return self._requires

    def __call__(self, /, *args: _P.args, **kwargs: _P.kwargs) -> WorkflowCall[_T]:
        """Bind arguments, producing an inert `WorkflowCall`.

        The workflow is not started here; pass the call to `duron.run`.

        Returns:
            An unstarted invocation carrying these arguments.

        """
        bound = self._signature.bind(*args, **kwargs)
        bound.apply_defaults()
        return WorkflowCall(
            self._durable,
            tuple(bound.args),
            dict(bound.kwargs),
            name=self.name,
            version=self.version,
            codec=self.codec,
            codec_id=self.codec_id,
        )

    @override
    def __repr__(self) -> str:
        return f"Workflow(name={self.name!r}, version={self.version!r})"


_WorkflowFn = Callable[Concatenate[WorkflowContext, _P], Coroutine[Any, Any, _T]]


@overload
def workflow(fn: _WorkflowFn[_P, _T], /) -> Workflow[_P, _T]: ...
@overload
def workflow(
    *,
    name: str | None = None,
    version: str = "1",
    codec: Codec | None = None,
    codec_id: str | None = None,
    requires: Sequence[Dependency] = (),
) -> Callable[[_WorkflowFn[_P, _T]], Workflow[_P, _T]]: ...
def workflow(
    fn: _WorkflowFn[_P, _T] | None = None,
    /,
    *,
    name: str | None = None,
    version: str = "1",
    codec: Codec | None = None,
    codec_id: str | None = None,
    requires: Sequence[Dependency] = (),
) -> Workflow[_P, _T] | Callable[[_WorkflowFn[_P, _T]], Workflow[_P, _T]]:
    """Mark an async function as a durable workflow.

    Annotate the first positional parameter as `WorkflowContext`. Decoration
    checks its annotation and rejects variadic parameters.
    ``name`` defaults to ``"{module}:{qualname}"``;
    ``version`` is the persisted public identity. Effects and ports are
    referenced from the body, not injected. List them in ``requires`` to make
    the workflow's external dependencies inspectable. The default declares no
    dependencies. Using an undeclared object emits a ``UserWarning`` once per
    object per workflow execution (including replay). Unused declarations are
    allowed for conditional paths. Python warning filters control reporting.

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

    def decorate(user_fn: _WorkflowFn[_P, _T]) -> Workflow[_P, _T]:
        if not inspect.iscoroutinefunction(user_fn):
            msg = "A workflow function must be defined with 'async def'"
            raise WorkflowDefinitionError(msg)
        _validate_context_param(user_fn)
        dependencies = tuple(requires)
        # Codec instances are user-provided objects and may intentionally be
        # false-y (for example by implementing ``__len__``). Presence, rather
        # than truthiness, selects the configured codec.
        resolved_codec = DefaultCodec if codec is None else codec
        resolved_codec_id = resolve_codec_id(resolved_codec, codec_id)
        resolved_name = name or default_name(user_fn)

        @functools.wraps(user_fn)
        async def shim(ctx: Context, *args: object, **kwargs: object) -> _T:
            wc: WorkflowContext = WorkflowContextImpl(
                ctx,
                resolved_codec,
                codec_id=resolved_codec_id,
                workflow_name=resolved_name,
                requires=dependencies,
            )
            call = cast("Callable[..., Coroutine[Any, Any, _T]]", user_fn)
            return await call(wc, *args, **kwargs)

        durable_fn: DurableFn[_P, _T] = DurableFn(
            codec=resolved_codec, fn=cast("Any", shim)
        )
        return Workflow(
            durable_fn,
            name=resolved_name,
            version=version,
            codec=resolved_codec,
            codec_id=resolved_codec_id,
            requires=dependencies,
        )

    if fn is not None:
        return decorate(fn)
    return decorate


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
