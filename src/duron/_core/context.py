from __future__ import annotations

import asyncio
import contextvars
import functools
import inspect
from collections.abc import AsyncGenerator
from random import Random
from typing import TYPE_CHECKING, Concatenate, cast, get_args, get_origin
from typing_extensions import Any, ParamSpec, TypeVar, final, overload

from duron._core.ops import (
    Barrier,
    FnCall,
    FutureComplete,
    FutureCreate,
    OpMetadata,
    create_op,
)
from duron._core.signal import create_signal
from duron._core.stream import create_stream, run_stateful
from duron._decorator.effect import Reducer
from duron.typing import UnspecifiedType, inspect_function

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Coroutine
    from contextlib import AbstractAsyncContextManager

    from duron._core.signal import Signal
    from duron._core.stream import Stream, StreamWriter
    from duron.loop import EventLoop
    from duron.typing import TypeHint

    _T = TypeVar("_T")
    _S = TypeVar("_S")
    _P = ParamSpec("_P")


@final
class Context:
    __slots__ = ("_loop", "_seed")

    def __init__(self, loop: EventLoop, seed: str) -> None:
        self._loop: EventLoop = loop
        self._seed: str = seed

    def _check(self) -> bool:
        if asyncio.get_running_loop() is not self._loop:
            msg = "Context time can only be used in the context loop"
            raise RuntimeError(msg)
        return True

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
        self,
        fn: Callable[Concatenate[_T, _P], AsyncGenerator[_S, _T]],
        /,
        state: _S,
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
        assert self._check()

        if inspect.isasyncgenfunction(fn):
            async with self.stream(fn, *args, **kwargs) as (stream, result):
                await stream.discard()
                return await result

        hint = inspect_function(fn)
        callable_ = functools.partial(fn, *args, **kwargs)

        op: asyncio.Future[object] = create_op(
            self._loop,
            FnCall(
                callable=callable_,
                return_type=hint.return_type,
                context=contextvars.copy_context(),
                metadata=OpMetadata(name=hint.name),
            ),
        )
        return await op

    def stream(
        self,
        fn: Callable[Concatenate[_T, _P], AsyncGenerator[_S, _T]],
        /,
        initial: _T,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> AbstractAsyncContextManager[tuple[Stream[_S], Awaitable[_T]]]:
        """Stream stateful function partial results.

        Args:
            fn: The stateful function to stream.
            initial: The initial state to pass to the function.
            *args: Positional arguments to pass to the function.
            **kwargs: Keyword arguments to pass to the function.

        Returns:
            An async context manager that yields a Stream of the function's results.

        """
        assert self._check()

        type_hint = inspect_function(fn)
        action_type: TypeHint[_S] = UnspecifiedType
        if get_origin(ret := type_hint.return_type) is AsyncGenerator:
            action_type, _ = get_args(ret)

        state_name = type_hint.parameters[0]
        annotations = type_hint.parameter_annotations.get(state_name, ())
        reducer = _find_reducer(tuple(annotations))
        return run_stateful(
            self._loop, action_type, reducer, fn, initial, *args, **kwargs
        )

    async def create_stream(
        self, dtype: TypeHint[_T], /, *, name: str | None = None
    ) -> tuple[Stream[_T], StreamWriter[_T]]:
        """Create a new stream within the context.

        Args:
            dtype: The data type of the stream.
            name: Optional name for the stream.

        Returns:
            Reader and writer for the created stream.

        """
        assert self._check()

        return await create_stream(
            self._loop, dtype, name, metadata=OpMetadata(name=name)
        )

    async def create_signal(
        self, dtype: TypeHint[_T], /, *, name: str | None = None
    ) -> tuple[Signal[_T], StreamWriter[_T]]:
        """Create a new signal within the context.

        Args:
            dtype: The data type of the stream.
            name: Optional name for the stream.

        Returns:
            Reader and writer for the created stream.

        """
        assert self._check()

        return await create_signal(
            self._loop, dtype, name, metadata=OpMetadata(name=name)
        )

    async def create_future(
        self, dtype: type[_T], /, *, name: str | None = None
    ) -> tuple[str, Awaitable[_T]]:
        """Create a new external future object within the context.

        Args:
            dtype: The data type of the stream.
            name: Optional name for the stream.

        Returns:
            Reader and writer for the created stream.

        """
        assert self._check()

        fut = create_op(
            self._loop, FutureCreate(return_type=dtype, metadata=OpMetadata(name=name))
        )
        return (fut.id, cast("asyncio.Future[_T]", fut))

    async def time(self) -> float:
        """Get the current deterministic time in seconds.

        This provides a deterministic timestamp that is consistent during replay.
        Use this instead of `time.time()` to ensure deterministic behavior.

        Returns:
            The current time in seconds as a float.

        """
        assert self._check()
        _log_offset, time_us = await create_op(self._loop, Barrier())
        return time_us * 1e-6

    async def time_ns(self) -> int:
        """Get the current deterministic time in nanoseconds.

        This provides a deterministic timestamp that is consistent during replay.
        Use this instead of `time.time_ns()` to ensure deterministic behavior.

        Returns:
            The current time in nanoseconds as an integer.

        """
        assert self._check()
        _log_offset, time_us = await create_op(self._loop, Barrier())
        return time_us * 1_000

    def random(self) -> Random:
        """Get a deterministic random number generator.

        This provides a seeded Random instance that produces consistent results
        during replay. Use this instead of the `random` module to ensure
        deterministic behavior.

        Returns:
            A Random instance seeded with a deterministic operation ID.

        """
        assert self._check()
        return Random(self._loop.generate_op_id() + self._seed)  # noqa: S311

    @overload
    async def complete_future(
        self, future_id: str, *, result: _T, result_type: TypeHint[_T] = ...
    ) -> None: ...
    @overload
    async def complete_future(
        self, future_id: str, *, exception: Exception
    ) -> None: ...
    async def complete_future(
        self,
        future_id: str,
        *,
        result: object | None = None,
        exception: Exception | None = None,
        result_type: TypeHint[object] = UnspecifiedType,
    ) -> None:
        """Complete an external future with a result or exception.

        This method completes a future that was created with `create_future()`,
        allowing external async work to integrate with duron's checkpointing.

        Args:
            future_id: The ID of the future to complete.
            result: The result value to set on the future.
            result_type: The type hint for the result value.
            exception: The exception to set on the future.

        """
        assert self._check()
        await create_op(
            self._loop,
            FutureComplete(
                future_id=future_id,
                value=result,
                exception=exception,
                dtype=result_type,
            ),
        )


def _find_reducer(annotations: tuple[Any, ...]) -> Callable[[_S, _T], _S]:
    for annotation in annotations:
        if isinstance(annotation, Reducer):
            return cast("Callable[[_S, _T], _S]", annotation.reducer)

    return cast("Callable[[_S, _T], _S]", _default_reducer)


def _default_reducer(_old: object, new: object) -> object:
    return new
