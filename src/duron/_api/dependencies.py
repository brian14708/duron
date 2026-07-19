"""Dependency declarations reuse existing effects and ports."""

from __future__ import annotations

from typing import Any, TypeAlias

from duron._api.ports import Input, Interrupt, Output, Request
from duron._runtime.execution.effect import Effect

Dependency: TypeAlias = (
    Effect[..., Any] | Input[Any] | Output[Any] | Request[Any, Any] | Interrupt[Any]
)
