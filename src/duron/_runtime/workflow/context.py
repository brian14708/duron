from __future__ import annotations

import asyncio
import contextvars
import functools
from random import Random
from typing import TYPE_CHECKING, Any, cast, final, overload
from typing_extensions import ParamSpec, TypeVar

from duron._hints import UnspecifiedType
from duron._introspection import inspect_function
from duron._runtime.workflow.ops import (
    Barrier,
    EffectCall,
    FnCall,
    FutureCreate,
    OpMetadata,
    OwnedChannelCreate,
    create_op,
)
from duron._runtime.workflow.signal import create_signal
from duron._runtime.workflow.stream import create_sink, create_stream

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Coroutine

    from duron._runtime.execution.executor import EffectRegistry
    from duron._runtime.execution.handle_data import HandleReference
    from duron._runtime.execution.models import EffectOperation
    from duron._runtime.loop import EventLoop
    from duron._runtime.workflow.signal import Signal
    from duron._runtime.workflow.stream import Stream, StreamWriter
    from duron.types import TypeHint

    _T = TypeVar("_T")
    _P = ParamSpec("_P")


@final
class Context:
    __slots__ = ("_loop", "_seed", "executor")

    def __init__(self, loop: EventLoop, seed: str, *, executor: EffectRegistry) -> None:
        self._loop: EventLoop = loop
        self._seed: str = seed
        self.executor = executor

    def _check(self) -> None:
        if asyncio.get_running_loop() is not self._loop:
            msg = "Context can only be used in its context loop"
            raise RuntimeError(msg)

    @property
    def loop(self) -> EventLoop:
        self._check()
        return self._loop

    async def open_owned_channel(
        self, reference: HandleReference, dtype: TypeHint[_T], name: str | None
    ) -> None:
        self._check()
        _ = await create_op(self._loop, OwnedChannelCreate(reference, dtype, name))

    @property
    def seed(self) -> str:
        """The run's seed nonce, part of its durable identity."""
        return self._seed

    @property
    def operation_id(self) -> str:
        """Deterministic id for the current operation position (non-consuming).

        Stable across replay of the same position; falls back to the run seed
        outside a durable task context.
        """
        op_id = self._loop.peek_op_id()
        return self._seed if op_id is None else op_id

    @overload
    async def run(
        self,
        fn: Callable[_P, Coroutine[Any, Any, _T]],
        /,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> _T: ...
    @overload
    async def run(
        self, fn: Callable[_P, _T], /, *args: _P.args, **kwargs: _P.kwargs
    ) -> _T: ...
    async def run(self, fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Any:
        """Run a function within the context.

        Returns:
            The result of the function call.

        """
        return await self._schedule(fn, args, kwargs)

    async def run_effect(
        self, operation: EffectOperation, return_type: TypeHint[_T]
    ) -> _T:
        """Schedule a registered effect with snapshotted caller inputs.

        Returns:
            The effect result.

        """
        self._check()
        result = await create_op(
            self._loop, EffectCall(operation, return_type, contextvars.copy_context())
        )
        return cast("_T", result)

    async def _schedule(
        self,
        fn: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        metadata: OpMetadata | None = None,
        return_type: TypeHint[Any] = UnspecifiedType,
    ) -> object:
        self._check()
        if metadata is None or return_type is UnspecifiedType:
            hint = inspect_function(fn)
            if metadata is None:
                metadata = OpMetadata(name=hint.name)
            if return_type is UnspecifiedType:
                return_type = hint.return_type
        op: asyncio.Future[object] = create_op(
            self._loop,
            FnCall(
                callable=functools.partial(fn, *args, **kwargs),
                return_type=return_type,
                context=contextvars.copy_context(),
                metadata=metadata,
            ),
        )
        return await op

    async def create_stream(
        self,
        dtype: TypeHint[_T],
        /,
        *,
        name: str | None = None,
        metadata: OpMetadata | None = None,
    ) -> tuple[Stream[_T], StreamWriter[_T]]:
        """Create a new stream within the context.

        Args:
            dtype: The data type of values emitted by the stream.
            name: Optional name for the stream.
            metadata: Optional durable operation identity and trace metadata.

        Returns:
            A stream reader and its writer.

        """
        self._check()

        return await create_stream(
            self._loop, dtype, name, metadata=metadata or OpMetadata(name=name)
        )

    async def create_sink(
        self,
        dtype: TypeHint[_T],
        /,
        *,
        name: str | None = None,
        metadata: OpMetadata | None = None,
    ) -> StreamWriter[_T]:
        """Create a write-only stream within the context.

        Emitted values are durably recorded without being buffered or decoded
        in the worker; host code reads them from storage.

        Args:
            dtype: The data type of values emitted by the stream.
            name: Optional name for the stream.
            metadata: Optional durable operation identity and trace metadata.

        Returns:
            A writer for the stream.

        """
        self._check()

        return await create_sink(
            self._loop, dtype, name, metadata=metadata or OpMetadata(name=name)
        )

    async def create_signal(
        self, dtype: TypeHint[_T], /, *, name: str | None = None
    ) -> tuple[Signal[_T], StreamWriter[_T]]:
        """Create a new signal within the context.

        Args:
            dtype: The data type of values emitted by the signal.
            name: Optional name for the signal.

        Returns:
            A signal reader and its writer.

        """
        self._check()

        return await create_signal(
            self._loop, dtype, name, metadata=OpMetadata(name=name)
        )

    async def create_future(
        self,
        dtype: type[_T],
        /,
        *,
        name: str | None = None,
        error_types: tuple[type[BaseException], ...] = (),
    ) -> tuple[str, Awaitable[_T]]:
        """Create a new external future object within the context.

        Args:
            dtype: The type of the future's result.
            name: Optional name for the future.
            error_types: Exception types the future may be failed with; they
                round-trip as themselves across replay.

        Returns:
            The future ID and awaitable that produces its result.

        """
        self._check()

        fut = create_op(
            self._loop,
            FutureCreate(
                return_type=dtype,
                metadata=OpMetadata(name=name, error_types=error_types),
            ),
        )
        return (fut.id, cast("asyncio.Future[_T]", fut))

    async def time_us(self) -> int:
        """Get the current deterministic time in microseconds.

        Same clock as :meth:`time`, in the log's native unit, so a consumer that
        needs a log timestamp need not convert back and forth through seconds.

        Returns:
            The current time as an integer number of microseconds.

        """
        self._check()
        _log_offset, time_us = await create_op(self._loop, Barrier())
        return time_us

    async def time(self) -> float:
        """Get the current deterministic time in seconds.

        This provides a deterministic timestamp that is consistent during replay.
        Use this instead of `time.time()` to ensure deterministic behavior.

        Returns:
            The current time in seconds as a float.

        """
        return (await self.time_us()) * 1e-6

    def random(self) -> Random:
        """Get a deterministic random number generator.

        This provides a seeded Random instance that produces consistent results
        during replay. Use this instead of the `random` module to ensure
        deterministic behavior.

        Returns:
            A Random instance seeded with a deterministic operation ID.

        """
        self._check()
        return Random(self._loop.generate_op_id() + self._seed)  # ruff: ignore[suspicious-non-cryptographic-random-usage]
