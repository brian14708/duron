"""Typed effect callables invoked from workflow code through ``ctx.call``."""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any, Concatenate, Generic, NoReturn, cast, overload
from typing_extensions import ParamSpec, Protocol, TypeVar, override

from duron._hints import UnspecifiedType
from duron._runtime.execution.definition import effect_signature
from duron._runtime.execution.handles import handle_annotation
from duron._runtime.execution.models import (
    EffectIdentity,
    RetryPolicy,
    validate_error_types,
)
from duron.errors import InvalidHandleError, WorkflowDefinitionError

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from duron._api.context import EffectContext
    from duron._runtime.execution.reporting import EffectExecutor
    from duron.codec import Codec
    from duron.types import TypeHint

_T = TypeVar("_T")
_P = ParamSpec("_P")

__all__ = ("Effect", "validate_error_types")


class Effect(Generic[_P, _T]):
    """An external or nondeterministic operation callable from a workflow.

    Invoke through `duron.WorkflowContext.call`. The original body is
    available as ``fn`` for tests supplying an object that implements the
    `duron.EffectContext` protocol.
    """

    __slots__ = (
        "_fn",
        "codec",
        "codec_id",
        "executor",
        "name",
        "raises",
        "retry",
        "signature",
        "version",
    )

    def __init__(
        self,
        fn: Callable[Concatenate[EffectContext, _P], Coroutine[Any, Any, _T]],
        *,
        name: str | None,
        version: str,
        raises: tuple[type[BaseException], ...] = (),
        retry: RetryPolicy | None = None,
        executor: EffectExecutor | None = None,
        codec: Codec | None = None,
        codec_id: str | None = None,
    ) -> None:
        self._fn = fn
        if not inspect.iscoroutinefunction(fn):
            msg = "an effect must be defined with async def"
            raise WorkflowDefinitionError(msg)
        signature = effect_signature(fn)
        self.signature = signature
        self.name = default_name(fn) if name is None else name
        self.version = version
        _ = EffectIdentity(self.name, version)
        self.retry = retry if retry is not None else RetryPolicy()
        self.executor = executor
        self.codec = codec
        self.codec_id = codec_id
        self.raises = validate_error_types(
            tuple(dict.fromkeys((*raises, *self.retry.retry_on))),
            f"effect {self.name!r}",
        )
        try:
            handles = [
                p
                for p in signature.parameters.values()
                if handle_annotation(p.annotation) is not None
            ]
        except (TypeError, ValueError, InvalidHandleError) as error:
            msg = "effect handle parameters require a concrete supported payload type"
            raise WorkflowDefinitionError(msg) from error
        if any(
            p.kind in {inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD}
            for p in handles
        ):
            msg = "runtime handles require named effect parameters"
            raise WorkflowDefinitionError(msg)

    @property
    def fn(self) -> Callable[Concatenate[EffectContext, _P], Coroutine[Any, Any, _T]]:
        return self._fn

    def __call__(self, *_args: _P.args, **_kwargs: _P.kwargs) -> NoReturn:
        msg = f"effect {self.name!r} was called directly; use ctx.call(effect, ...)"
        raise WorkflowDefinitionError(msg)

    @property
    def identity(self) -> EffectIdentity:
        return EffectIdentity(self.name, self.version)

    @property
    def return_type(self) -> TypeHint[_T]:
        annotation = self.signature.return_annotation
        return cast(
            "TypeHint[_T]",
            UnspecifiedType if annotation is inspect.Signature.empty else annotation,
        )

    @override
    def __repr__(self) -> str:
        return f"Effect({self.name!r})"


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


class _EffectDecorator(Protocol):
    def __call__(
        self, fn: Callable[Concatenate[EffectContext, _P], Coroutine[Any, Any, _T]], /
    ) -> Effect[_P, _T]: ...


@overload
def effect(
    fn: Callable[Concatenate[EffectContext, _P], Coroutine[Any, Any, _T]], /
) -> Effect[_P, _T]: ...
@overload
def effect(
    *,
    name: str | None = None,
    version: str = "1",
    raises: tuple[type[BaseException], ...] = (),
    retry: RetryPolicy | None = None,
    executor: EffectExecutor | None = None,
    codec: Codec | None = None,
    codec_id: str | None = None,
) -> _EffectDecorator: ...
def effect(
    fn: Callable[Concatenate[EffectContext, _P], Coroutine[Any, Any, _T]] | None = None,
    /,
    *,
    name: str | None = None,
    version: str = "1",
    raises: tuple[type[BaseException], ...] = (),
    retry: RetryPolicy | None = None,
    executor: EffectExecutor | None = None,
    codec: Codec | None = None,
    codec_id: str | None = None,
) -> Effect[_P, _T] | _EffectDecorator:
    """Mark a callable as an effect.

    Effects interact with the outside world. Only ``@effect`` callables are
    accepted by `duron.WorkflowContext.call`, which prevents recording
    arbitrary helpers as external effects.

    Effects must be async functions. Applications choose how to offload blocking work.

    Args:
        fn: The callable to wrap.
        name: Stable effect name recorded in history. Defaults to
            ``"{module}:{qualname}"``.
        version: Stable effect version recorded in history.
        raises: Exception types the effect is expected to fail with. Declared
            types (and built-in exceptions) round-trip as themselves when a
            recorded failure is replayed; undeclared types are re-raised as
            `duron.errors.RemoteEffectError`.
        retry: Retry and recovery policy.
        executor: Execution environment for this effect. Omit for local execution.
        codec: Effect-specific codec. Defaults to the workflow codec.
        codec_id: Stable identifier required for a custom codec.

    Returns:
        An `Effect`, or a decorator producing one.

    """

    def decorate(
        inner: Callable[Concatenate[EffectContext, _P], Coroutine[Any, Any, _T]],
    ) -> Effect[_P, _T]:
        return Effect(
            inner,
            name=name,
            version=version,
            raises=raises,
            retry=retry,
            executor=executor,
            codec=codec,
            codec_id=codec_id,
        )

    if fn is not None:
        return decorate(fn)
    return cast("_EffectDecorator", decorate)
