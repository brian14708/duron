from __future__ import annotations

import asyncio
import contextvars
import functools
from random import Random
from typing import TYPE_CHECKING, cast
from typing_extensions import Any, ParamSpec, TypeVar, final, overload

from duron._core.ops import Barrier, FnCall, FutureCreate, OpMetadata, create_op
from duron._core.signal import create_signal
from duron._core.stream import create_sink, create_stream
from duron.typing import UnspecifiedType, inspect_function

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Coroutine

    from duron._core.signal import Signal
    from duron._core.stream import Stream, StreamWriter
    from duron.loop import EventLoop
    from duron.typing import TypeHint

    _T = TypeVar("_T")
    _P = ParamSpec("_P")


@final
class Context:
    __slots__ = ("_loop", "_seed")

    def __init__(self, loop: EventLoop, seed: str) -> None:
        self._loop: EventLoop = loop
        self._seed: str = seed

    def _check(self) -> None:
        if asyncio.get_running_loop() is not self._loop:
            msg = "Context can only be used in its context loop"
            raise RuntimeError(msg)

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

    @overload
    async def run_effect(
        self,
        metadata: OpMetadata,
        fn: Callable[..., Coroutine[Any, Any, _T]],
        /,
        *args: object,
        return_type: TypeHint[Any] = ...,
        **kwargs: object,
    ) -> _T: ...
    @overload
    async def run_effect(
        self,
        metadata: OpMetadata,
        fn: Callable[..., _T],
        /,
        *args: object,
        return_type: TypeHint[Any] = ...,
        **kwargs: object,
    ) -> _T: ...
    async def run_effect(
        self,
        metadata: OpMetadata,
        fn: Callable[..., object],
        /,
        *args: object,
        return_type: TypeHint[Any] = UnspecifiedType,
        **kwargs: object,
    ) -> object:
        """Run an effect with its stable public identity attached.

        ``return_type`` lets callers that wrap the effect in a synthetic
        callable supply the original declared return type directly, instead of
        having it re-derived (untyped) from the wrapper's signature.

        Returns:
            The effect result.

        """
        return await self._schedule(fn, args, kwargs, metadata, return_type)

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
        adopt_replayed: bool = False,
    ) -> tuple[Stream[_T], StreamWriter[_T]]:
        """Create a new stream within the context.

        Args:
            dtype: The data type of values emitted by the stream.
            name: Optional name for the stream.
            metadata: Optional durable operation identity and trace metadata.
            adopt_replayed: Set when the stream is produced by an external
                producer that re-runs from the start on resume (a streaming
                effect call): the writer then skips re-recording the events
                its prior execution already recorded before a crash.

        Returns:
            A stream reader and its writer.

        """
        self._check()

        return await create_stream(
            self._loop,
            dtype,
            name,
            metadata=metadata or OpMetadata(name=name),
            adopt_replayed=adopt_replayed,
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
        return Random(self._loop.generate_op_id() + self._seed)  # noqa: S311
