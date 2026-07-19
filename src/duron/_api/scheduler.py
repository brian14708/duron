"""Typed scheduler contract."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Self
from typing_extensions import ParamSpec, TypeVar

from duron._api.ports import NoPorts, PortSet

if TYPE_CHECKING:
    from types import TracebackType

    from duron._api.reference import WorkflowRef
    from duron._api.run import Run
    from duron._api.workflow import Workflow

_P = ParamSpec("_P")
_T = TypeVar("_T")
_Ports = TypeVar("_Ports", bound=PortSet, default=NoPorts)


class Scheduler(ABC):
    """Abstract scheduler contract for typed submission and observation."""

    @abstractmethod
    async def __aenter__(self) -> Self: ...

    @abstractmethod
    async def __aexit__(
        self,
        _type: type[BaseException] | None,
        _value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None: ...

    @abstractmethod
    async def submit(
        self,
        workflow: Workflow[_P, _T, _Ports] | WorkflowRef[_P, _T, _Ports],
        run_id: str,
        /,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> Run[_T, _Ports]: ...

    @abstractmethod
    async def open(
        self,
        workflow: Workflow[_P, _T, _Ports] | WorkflowRef[_P, _T, _Ports],
        run_id: str,
        /,
    ) -> Run[_T, _Ports]: ...


__all__ = ("Scheduler",)
