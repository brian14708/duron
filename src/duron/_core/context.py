from __future__ import annotations

import asyncio
import contextvars
import functools
import inspect
from random import Random
from typing import TYPE_CHECKING, cast
from typing_extensions import (
    Any,
    ParamSpec,
    TypeVar,
    final,
    overload,
)

from duron._core.ops import (
    Barrier,
    FnCall,
    FutureComplete,
    FutureCreate,
    OpAnnotations,
    create_op,
)
from duron._core.signal import create_signal
from duron._core.stream import create_stream, run_stateful
from duron._decorator.effect import EffectFn, StatefulFn
from duron._loop import EventLoop
from duron.typing import inspect_function

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Coroutine, Mapping
    from contextlib import AbstractAsyncContextManager

    from duron._core.signal import Signal, SignalWriter
    from duron._core.stream import Stream, StreamWriter
    from duron.typing import TypeHint

    _T = TypeVar("_T")
    _S = TypeVar("_S")
    _P = ParamSpec("_P")


@final
class Context:
    __slots__ = ("_loop", "_token")

    def __init__(self, loop: EventLoop) -> None:
        self._loop: EventLoop = loop

    @staticmethod
    def current() -> Context:
        """Get the currently active Context.

        Returns:
            The active Context instance.

        Raises:
            RuntimeError: If no duron context is currently active.
        """
        loop = asyncio.get_running_loop()
        if not isinstance(loop, EventLoop):
            msg = "Context requires a duron EventLoop"
            raise RuntimeError(msg)  # noqa: TRY004
        return Context(loop)

    @overload
    async def run(
        self,
        fn: Callable[_P, Coroutine[Any, Any, _T]] | EffectFn[_P, _T],
        /,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> _T: ...
    @overload
    async def run(
        self,
        fn: Callable[_P, _T] | StatefulFn[_P, _T, Any],
        /,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> _T: ...
    async def run(
        self,
        fn: Callable[_P, Coroutine[Any, Any, _T] | _T]
        | EffectFn[_P, _T]
        | StatefulFn[_P, _T, Any],
        /,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> _T:
        """
        Run a function within the context.

        Returns:
            The result of the function call.

        Raises:
            RuntimeError: If called outside of the context's event loop.
        """
        if asyncio.get_running_loop() is not self._loop:
            msg = "Context time can only be used in the context loop"
            raise RuntimeError(msg)

        if isinstance(fn, StatefulFn):
            async with self.stream(
                cast("StatefulFn[_P, _T, Any]", fn), *args, **kwargs
            ) as stream:
                await stream.discard()
                return await stream

        if isinstance(fn, EffectFn):
            callable_ = fn.fn
            return_type = fn.return_type
        elif inspect.iscoroutinefunction(fn):
            callable_ = fn
            return_type = inspect_function(fn).return_type
        else:

            async def wrapper(  # noqa: RUF029
                *args: _P.args, **kwargs: _P.kwargs
            ) -> object:
                return fn(*args, **kwargs)

            callable_ = functools.update_wrapper(wrapper, fn)
            return_type = inspect_function(fn).return_type

        op = create_op(
            self._loop,
            FnCall(
                callable=callable_,
                args=args,
                kwargs=kwargs,
                return_type=return_type,
                context=contextvars.copy_context(),
                annotations=OpAnnotations(
                    name=cast("str", getattr(callable_, "__name__", repr(callable_)))
                ),
            ),
        )
        return cast("_T", await op)

    def stream(
        self, fn: StatefulFn[_P, _T, _S], /, *args: _P.args, **kwargs: _P.kwargs
    ) -> AbstractAsyncContextManager[Stream[_S, _T]]:
        """Stream stateful function partial results.

        Args:
            fn: The stateful function to stream.
            *args: Positional arguments to pass to the function.
            **kwargs: Keyword arguments to pass to the function.

        Raises:
            RuntimeError: If called outside of the context's event loop.

        Returns:
            An async context manager that yields a Stream of the function's results.
        """
        if asyncio.get_running_loop() is not self._loop:
            msg = "Context time can only be used in the context loop"
            raise RuntimeError(msg)
        return run_stateful(
            self._loop, fn.action_type, fn.initial(), fn.reducer, fn.fn, *args, **kwargs
        )

    async def create_stream(
        self,
        dtype: TypeHint[_T],
        /,
        *,
        name: str | None = None,
        labels: Mapping[str, str] | None = None,
    ) -> tuple[Stream[_T, None], StreamWriter[_T]]:
        """Create a new stream within the context.

        Args:
            dtype: The data type of the stream.
            name: Optional name for the stream.
            labels: Optional labels for the stream.

        Raises:
            RuntimeError: If called outside of the context's event loop.

        Returns:
            Reader and writer for the created stream.
        """
        if asyncio.get_running_loop() is not self._loop:
            msg = "Context time can only be used in the context loop"
            raise RuntimeError(msg)

        annotations = OpAnnotations()
        if labels:
            annotations = OpAnnotations.extend(annotations, labels=labels)
        if name:
            annotations = OpAnnotations.extend(
                annotations, name=name, labels={"name": name}
            )
        return await create_stream(self._loop, dtype, annotations=annotations)

    async def create_signal(
        self,
        dtype: TypeHint[_T],
        /,
        *,
        name: str | None = None,
        labels: Mapping[str, str] | None = None,
    ) -> tuple[Signal[_T], SignalWriter[_T]]:
        """Create a new signal within the context.

        Args:
            dtype: The data type of the stream.
            name: Optional name for the stream.
            labels: Optional labels for the stream.

        Raises:
            RuntimeError: If called outside of the context's event loop.

        Returns:
            Reader and writer for the created stream.
        """
        if asyncio.get_running_loop() is not self._loop:
            msg = "Context time can only be used in the context loop"
            raise RuntimeError(msg)

        annotations = OpAnnotations()
        if labels:
            annotations = OpAnnotations.extend(annotations, labels=labels)
        if name:
            annotations = OpAnnotations.extend(
                annotations, name=name, labels={"name": name}
            )

        return await create_signal(self._loop, dtype, annotations=annotations)

    async def create_future(
        self,
        dtype: type[_T],
        /,
        *,
        name: str | None = None,
        labels: Mapping[str, str] | None = None,
    ) -> tuple[str, Awaitable[_T]]:
        """Create a new external future object within the context.

        Args:
            dtype: The data type of the stream.
            name: Optional name for the stream.
            labels: Optional labels for the stream.

        Raises:
            RuntimeError: If called outside of the context's event loop.

        Returns:
            Reader and writer for the created stream.
        """
        if asyncio.get_running_loop() is not self._loop:
            msg = "Context time can only be used in the context loop"
            raise RuntimeError(msg)

        annotations = OpAnnotations()
        if labels:
            annotations = annotations.extend(labels=labels)
        if name:
            annotations = annotations.extend(name=name, labels={"name": name})
        fut = create_op(
            self._loop, FutureCreate(return_type=dtype, annotations=annotations)
        )
        return (fut.id, cast("asyncio.Future[_T]", fut))

    async def barrier(self) -> int:
        """Create a barrier operation that records the current execution offset.

        Raises:
            RuntimeError: If called outside of the context's event loop.

        Returns:
            The log offset at the point of the barrier.
        """
        if asyncio.get_running_loop() is not self._loop:
            msg = "Context time can only be used in the context loop"
            raise RuntimeError(msg)
        return await create_op(self._loop, Barrier())

    def time(self) -> float:
        """Get the current deterministic time in seconds.

        This provides a deterministic timestamp that is consistent during replay.
        Use this instead of `time.time()` to ensure deterministic behavior.

        Raises:
            RuntimeError: If called outside of the context's event loop.

        Returns:
            The current time in seconds as a float.
        """
        if asyncio.get_running_loop() is not self._loop:
            msg = "Context time can only be used in the context loop"
            raise RuntimeError(msg)
        return self._loop.time()

    def time_ns(self) -> int:
        """Get the current deterministic time in nanoseconds.

        This provides a deterministic timestamp that is consistent during replay.
        Use this instead of `time.time_ns()` to ensure deterministic behavior.

        Raises:
            RuntimeError: If called outside of the context's event loop.

        Returns:
            The current time in nanoseconds as an integer.
        """
        if asyncio.get_running_loop() is not self._loop:
            msg = "Context time can only be used in the context loop"
            raise RuntimeError(msg)
        return self._loop.time_us() * 1_000

    def random(self) -> Random:
        """Get a deterministic random number generator.

        This provides a seeded Random instance that produces consistent results
        during replay. Use this instead of the `random` module to ensure
        deterministic behavior.

        Raises:
            RuntimeError: If called outside of the context's event loop.

        Returns:
            A Random instance seeded with a deterministic operation ID.
        """
        if asyncio.get_running_loop() is not self._loop:
            msg = "Context random can only be used in the context loop"
            raise RuntimeError(msg)
        return Random(self._loop.generate_op_id())  # noqa: S311

    @overload
    async def complete_future(self, future_id: str, *, result: object) -> None: ...
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
    ) -> None:
        """Complete an external future with a result or exception.

        This method completes a future that was created with `create_future()`,
        allowing external async work to integrate with duron's checkpointing.

        Args:
            future_id: The ID of the future to complete.
            result: The result value to set on the future.
            exception: The exception to set on the future.
        """
        await create_op(
            self._loop,
            FutureComplete(future_id=future_id, value=result, exception=exception),
        )
