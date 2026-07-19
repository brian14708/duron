"""Exercise native annotation evaluation, including deferred annotations."""

# Deliberately omit future annotations to exercise Python 3.14+ evaluation.

import asyncio
from typing import Annotated
from typing_extensions import TypedDict

import duron


class Ports(duron.PortSet):
    ready = duron.Output[int]()
    finish = duron.Input[bool]()


def define_grouped_workflow() -> duron.Workflow[..., int, Ports]:
    # Deferred annotations must retain names from the definition's own scope.
    class Payload(TypedDict):
        value: int

    @duron.effect
    async def double(
        _ctx: duron.EffectContext, payload: Annotated[Payload, "payload"]
    ) -> int:
        return payload["value"] * 2

    @duron.workflow(ports=Ports)
    async def grouped(ctx: duron.WorkflowContext, payload: Payload) -> int:
        async with asyncio.TaskGroup() as tasks:
            first = tasks.create_task(ctx.call(double, payload))
            second = tasks.create_task(
                ctx.call(double, {"value": payload["value"] + 1})
            )
        result = first.result() + second.result()
        await ctx.port(Ports.ready).emit(result)
        await ctx.port(Ports.finish).receive()
        return result

    return grouped
