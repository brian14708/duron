"""Typed effect callables invoked from workflow code through ``ctx.call``."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any, Concatenate, Generic, NoReturn, cast, overload
from typing_extensions import ParamSpec, Protocol, TypeVar, override

from duron._api.context import EffectContext
from duron._api.ports import Input, _declared_ports
from duron._execution.handles import SignalReceiver, StreamSender, handle_annotation
from duron._hints import UnspecifiedType
from duron._introspection import effect_signature
from duron._ref import ref_of
from duron.errors import InvalidHandleError, WorkflowDefinitionError

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from duron._api.ports import PortSet, _Port
    from duron._execution.invocation import Executor
    from duron._hints import TypeHint

_T_co = TypeVar("_T_co", covariant=True, default=object)
_P = ParamSpec("_P", default=...)

__all__ = ("Effect", "effect")


class Effect(Generic[_P, _T_co]):
    """An external or nondeterministic operation callable from a workflow.

    Invoke through `duron.WorkflowContext.call`. The original body is
    available as ``fn`` for tests supplying an object that implements the
    `duron.EffectContext` protocol.
    """

    __slots__ = ("_executor", "_fn", "_name", "_ports", "_signature", "_wire_signature")

    def __init__(
        self,
        fn: Callable[Concatenate[EffectContext, _P], Coroutine[Any, Any, _T_co]],
        *,
        name: str | None,
        executor: Executor | None = None,
        ports: type[PortSet] | None = None,
    ) -> None:
        self._fn = fn
        if not inspect.iscoroutinefunction(fn):
            msg = "an effect must be defined with async def"
            raise WorkflowDefinitionError(msg)
        signature = effect_signature(fn, context_type=EffectContext)
        self._signature = signature
        self._ports = _declared_ports(ports)
        parameters = list(signature.parameters.values())
        for port in self.ports:
            if isinstance(port, Input):
                annotation: object = cast("Any", SignalReceiver)[port.item_type]
            else:
                annotation = cast("Any", StreamSender)[port.item_type]
            parameters.append(
                inspect.Parameter(
                    f"_duron_port_{port.name}",
                    inspect.Parameter.KEYWORD_ONLY,
                    annotation=annotation,
                )
            )
        if any(p.name.startswith("_duron_") for p in signature.parameters.values()):
            msg_0 = "effect parameter names beginning _duron_ are reserved"
            raise WorkflowDefinitionError(msg_0)
        self._wire_signature = signature.replace(parameters=parameters)
        ref = ref_of(fn, name=name)
        if not ref["name"]:
            msg = "effect name must be nonempty"
            raise WorkflowDefinitionError(msg)
        self._name = ref["name"]
        self._executor = executor
        try:
            handles = [
                p
                for p in signature.parameters.values()
                if handle_annotation(p.annotation) is not None
            ]
        except (TypeError, ValueError, InvalidHandleError) as error:
            msg = "effect handle parameters require a concrete supported payload type"
            raise WorkflowDefinitionError(msg) from error
        if handles:
            msg_0 = "declare effect ports instead of handle parameters"
            raise WorkflowDefinitionError(msg_0)

    @property
    def fn(
        self,
    ) -> Callable[Concatenate[EffectContext, _P], Coroutine[Any, Any, _T_co]]:
        return self._fn

    @property
    def name(self) -> str:
        """Stable effect name recorded in history."""
        return self._name

    @property
    def signature(self) -> inspect.Signature:
        """The inspected effect signature."""
        return self._signature

    @property
    def executor(self) -> Executor | None:
        """Selected execution environment, if any."""
        return self._executor

    @property
    def ports(self) -> tuple[_Port, ...]:
        """The effect's declared ports, in declaration order."""
        return self._ports

    def __call__(self, *_args: _P.args, **_kwargs: _P.kwargs) -> NoReturn:
        msg = f"effect {self.name!r} was called directly; use ctx.call(effect, ...)"
        raise WorkflowDefinitionError(msg)

    @property
    def return_type(self) -> TypeHint[_T_co]:
        annotation = self.signature.return_annotation
        return cast(
            "TypeHint[_T_co]",
            UnspecifiedType if annotation is inspect.Signature.empty else annotation,
        )

    @override
    def __repr__(self) -> str:
        return f"Effect({self.name!r})"


class _EffectDecorator(Protocol):
    def __call__(
        self,
        fn: Callable[Concatenate[EffectContext, _P], Coroutine[Any, Any, _T_co]],
        /,
    ) -> Effect[_P, _T_co]: ...


@overload
def effect(
    fn: Callable[Concatenate[EffectContext, _P], Coroutine[Any, Any, _T_co]], /
) -> Effect[_P, _T_co]: ...
@overload
def effect(
    *,
    name: str | None = None,
    executor: Executor | None = None,
    ports: type[PortSet] | None = None,
) -> _EffectDecorator: ...
def effect(
    fn: Callable[Concatenate[EffectContext, _P], Coroutine[Any, Any, _T_co]]
    | None = None,
    /,
    *,
    name: str | None = None,
    executor: Executor | None = None,
    ports: type[PortSet] | None = None,
) -> Effect[..., _T_co] | _EffectDecorator:
    """Mark a callable as an effect.

    Effects interact with the outside world. Only ``@effect`` callables are
    accepted by `duron.WorkflowContext.call`, which prevents recording
    arbitrary helpers as external effects.

    Effects must be async functions. Applications choose how to offload blocking work.

    Args:
        fn: The callable to wrap.
        name: Stable effect name recorded in history. Defaults to
            ``"{module}:{qualname}"``.
        executor: Execution environment for this effect. Omit for local execution.
        ports: PortSet declaring the effect's input and output ports.

    Returns:
        An `Effect`, or a decorator producing one.

    """

    def decorate(
        inner: Callable[Concatenate[EffectContext, _P], Coroutine[Any, Any, _T_co]],
    ) -> Effect[_P, _T_co]:
        return Effect(inner, name=name, executor=executor, ports=ports)

    if fn is not None:
        return decorate(fn)
    return cast("_EffectDecorator", decorate)
