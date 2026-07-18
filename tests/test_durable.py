from __future__ import annotations

import pytest

from duron import Context, DurableDefinitionError, Provided, Stream, durable


def test_durable_rejects_sync_function() -> None:
    with pytest.raises(DurableDefinitionError, match="async def"):

        @durable  # type: ignore[arg-type]
        def invalid(_ctx: Context) -> None:  # pyright: ignore[reportUnusedFunction]
            pass


def test_durable_requires_context_first_parameter() -> None:
    with pytest.raises(DurableDefinitionError, match=r"Context.*first"):

        @durable  # type: ignore[arg-type]
        async def missing_context() -> None:  # pyright: ignore[reportUnusedFunction]
            pass

    with pytest.raises(DurableDefinitionError, match=r"first.*Context"):

        @durable
        async def wrong_context(_ctx: object) -> None:  # pyright: ignore[reportUnusedFunction]
            pass


def test_durable_rejects_variadic_parameters() -> None:
    with pytest.raises(DurableDefinitionError, match="Variadic parameter"):

        @durable
        async def invalid(_ctx: Context, *values: int) -> None:  # pyright: ignore[reportUnusedFunction]
            _ = values


def test_durable_requires_provided_for_injected_parameter() -> None:
    with pytest.raises(DurableDefinitionError, match=r"'stream'.*Provided"):

        @durable
        async def invalid(_ctx: Context, stream: Stream[int]) -> None:  # pyright: ignore[reportUnusedFunction]
            _ = stream


def test_durable_preserves_function_metadata_and_valid_injection() -> None:
    @durable
    async def workflow(
        _ctx: Context, /, value: int, *, _stream: Stream[int] = Provided
    ) -> int:
        """Workflow documentation.

        Returns:
            The input value.

        """
        return value

    assert getattr(workflow.fn, "__name__", None) == "workflow"
    assert (workflow.fn.__doc__ or "").startswith("Workflow documentation.")
    assert workflow.inject[0][0] == "_stream"


def test_durable_accepts_unresolved_postponed_annotations() -> None:
    namespace: dict[str, object] = {}
    exec(  # noqa: S102
        """
from __future__ import annotations
from duron import Context, durable

@durable
async def workflow(ctx: Context, value: MissingType) -> None:
    pass
""",
        namespace,
    )
    assert namespace["workflow"] is not None
