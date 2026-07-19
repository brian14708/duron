"""Typed effect callables invoked from workflow code through ``ctx.call``."""

from __future__ import annotations

import asyncio
import inspect
from typing import TYPE_CHECKING, Generic, Literal, cast, get_args
from typing_extensions import Any, ParamSpec, Protocol, TypeVar, overload, override

from duron._core.ops import OpMetadata
from duron.errors import WorkflowDefinitionError
from duron.loop import EventLoop
from duron.typing import UnspecifiedType, inspect_function

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from duron.typing import TypeHint

_T = TypeVar("_T")
_P = ParamSpec("_P")

Executor = Literal["inline", "thread"]


class Effect(Generic[_P, _T]):
    """An external or nondeterministic operation callable from a workflow.

    Instances are created by :func:`effect`. They remain ordinary callables so
    they can also be used directly outside a workflow, but inside a workflow
    they are invoked through :meth:`~duron.WorkflowContext.call`.
    """

    __slots__ = (
        "_fn",
        "executor",
        "is_async",
        "is_generator",
        "name",
        "raises",
        "version",
    )

    def __init__(
        self,
        fn: Callable[_P, Any],
        *,
        name: str | None,
        version: str,
        executor: Executor | None,
        raises: tuple[type[BaseException], ...] = (),
    ) -> None:
        self._fn = fn
        self.name = name or default_name(fn)
        self.version = version
        self.raises = validate_error_types(raises, f"effect {self.name!r}")
        self.is_async = inspect.iscoroutinefunction(fn)
        self.is_generator = inspect.isasyncgenfunction(fn)
        if not self.is_async and not self.is_generator and executor is None:
            msg = (
                f"Synchronous effect {self.name!r} requires an explicit executor "
                'policy, e.g. @duron.effect(executor="thread").'
            )
            raise WorkflowDefinitionError(msg)
        self.executor: Executor = executor or "inline"

    @property
    def fn(self) -> Callable[_P, Any]:
        return self._fn

    def __call__(self, *args: _P.args, **kwargs: _P.kwargs) -> Any:  # noqa: ANN401
        _reject_call_inside_workflow(self.name)
        return self._fn(*args, **kwargs)

    @property
    def return_type(self) -> TypeHint[_T]:
        return cast("TypeHint[_T]", inspect_function(self._fn).return_type)

    @property
    def op_metadata(self) -> OpMetadata:
        """The durable identity metadata recorded on ops running this effect."""
        return OpMetadata(
            name=self.name,
            effect_name=self.name,
            effect_version=self.version,
            error_types=self.raises,
        )

    @override
    def __repr__(self) -> str:
        return f"Effect({self.name!r})"


def _reject_call_inside_workflow(name: str) -> None:
    """Reject calling an effect directly on the durable workflow loop.

    Workflow bodies run on the durable :class:`~duron.loop.EventLoop`; effect
    bodies run on the host loop. So a running :class:`EventLoop` means an effect
    was invoked directly from workflow code (``await eff(...)``) instead of
    through ``ctx.call``, which would skip durable recording and break replay.

    Raises:
        WorkflowDefinitionError: if invoked on the durable workflow loop.

    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    if isinstance(loop, EventLoop):
        msg = (
            f"effect {name!r} was called directly inside a workflow; use "
            "ctx.call(effect, ...) (or ctx.stream) so its result is durably recorded"
        )
        raise WorkflowDefinitionError(msg)


def validate_error_types(
    types: tuple[type[BaseException], ...], owner: str
) -> tuple[type[BaseException], ...]:
    # Cast away the declared element type: this validation exists precisely
    # for callers that bypass the annotation.
    for t in cast("tuple[object, ...]", types):
        if not (isinstance(t, type) and issubclass(t, BaseException)):
            msg = f"{owner} declares a non-exception error type: {t!r}"
            raise WorkflowDefinitionError(msg)
    return types


def default_name(fn: Callable[..., Any]) -> str:
    """Return the ``"{module}:{qualname}"`` identity recorded for a callable.

    Returns:
        The stable default name for ``fn``.

    """
    module = cast("str", getattr(fn, "__module__", None) or "?")
    qualname = cast(
        "str", getattr(fn, "__qualname__", None) or getattr(fn, "__name__", repr(fn))
    )
    return f"{module}:{qualname}"


def item_type_of(effect: Effect[Any, Any]) -> TypeHint[Any]:
    """Return the item type yielded by a streaming (async-generator) effect.

    Returns:
        The declared item type, or unspecified when it cannot be determined.

    """
    ret = inspect_function(effect.fn).return_type
    args = get_args(ret)
    return cast("TypeHint[Any]", args[0]) if args else UnspecifiedType


class _EffectDecorator(Protocol):
    @overload
    def __call__(
        self, fn: Callable[_P, Coroutine[Any, Any, _T]], /
    ) -> Effect[_P, _T]: ...
    @overload
    def __call__(self, fn: Callable[_P, _T], /) -> Effect[_P, _T]: ...


@overload
def effect(fn: Callable[_P, Coroutine[Any, Any, _T]], /) -> Effect[_P, _T]: ...
@overload
def effect(fn: Callable[_P, _T], /) -> Effect[_P, _T]: ...
@overload
def effect(
    *,
    name: str | None = None,
    version: str = "1",
    executor: Executor | None = None,
    raises: tuple[type[BaseException], ...] = (),
) -> _EffectDecorator: ...
def effect(  # type: ignore[misc]
    fn: Callable[_P, _T] | None = None,
    /,
    *,
    name: str | None = None,
    version: str = "1",
    executor: Executor | None = None,
    raises: tuple[type[BaseException], ...] = (),
) -> Effect[_P, _T] | _EffectDecorator:
    """Mark a callable as an effect.

    Effects interact with the outside world. Only ``@effect`` callables are
    accepted by :meth:`~duron.WorkflowContext.call`, which prevents recording
    arbitrary helpers as external effects.

    Synchronous effects require an explicit executor policy (``"inline"`` or
    ``"thread"``); ``"inline"`` blocks the worker event loop and is never
    selected implicitly.

    Args:
        fn: The callable to wrap.
        name: Stable effect name recorded in history. Defaults to
            ``"{module}:{qualname}"``.
        version: Stable effect version recorded in history.
        executor: Execution policy for synchronous effects.
        raises: Exception types the effect is expected to fail with. Declared
            types (and built-in exceptions) round-trip as themselves when a
            recorded failure is replayed; undeclared types are re-raised as
            :class:`~duron.RemoteEffectError`.

    Returns:
        An :class:`Effect`, or a decorator producing one.

    """

    def decorate(inner: Callable[_P, _T]) -> Effect[_P, _T]:
        return Effect(
            inner, name=name, version=version, executor=executor, raises=raises
        )

    if fn is not None:
        return decorate(fn)
    return cast("_EffectDecorator", decorate)
