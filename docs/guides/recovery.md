# Replay, recovery, and application services

## Use durable time and randomness

Inside workflows, use `await ctx.now()` for deterministic UTC time and
`ctx.random()` for a replay-stable random generator. Calculate elapsed seconds
with `((await ctx.now()) - started_at).total_seconds()`, where `started_at` is
an earlier `await ctx.now()` value. Use `await ctx.sleep(seconds)` for a durable
deadline. A restart waits only for the remainder of that deadline.
Durations must be finite nonnegative numbers; booleans, negatives, NaN, and
infinities raise `ValueError`.

Inside effects and host tasks, use ordinary clocks, random generators, and
`asyncio.sleep`.

## Recover interrupted effects

When a run resumes with an unreported effect outcome, Duron invokes the same
durable call again. Recovery has no invocation limit. Completed effects replay
their recorded outcomes, and reported application failures are final.
An effect result that fails declared-type or codec validation is recorded as a
terminal application failure rather than automatically invoking the effect again.

For external mutations, pass stable application operation IDs as effect
arguments and use them to deduplicate requests at the external service. Reuse
the same application ID for repeated requests representing the same action.

## Supply typed application services

An effect can close over an application service. Construct the definition with
the service it will use:

```python
from typing import Protocol

from duron import Executor


class OrderClient(Protocol):
    async def fetch(self, key: str) -> str: ...


def make_fetch(
    client: OrderClient, executor: Executor | None = None
) -> duron.Effect[[str], str]:
    @duron.effect(name="orders.fetch", version="0", executor=executor)
    async def fetch(_ctx: duron.EffectContext, key: str) -> str:
        return await client.fetch(key)

    return fetch
```

Manage the service lifetime in the host that constructs the effect. Ordinary
local effects pass no executor. The run owner can use
`make_fetch(client, dispatcher)` to select an adapter for this effect.
Pass workflow definitions to `SQLiteScheduler(path, workflows=[...])`; effect executors
remain selected on each effect definition.

`AsyncExecutor` runs the registered Python function by default.
A remote executor uses `invocation.arguments`, `metadata`, and `port(...)`, then
returns its result or raises `ApplicationFailure(error)`. See the
[execution reference](../reference/execution.md).

## Use a custom codec

Implement `duron.codec.Codec.encode_json(value, annotated_type)` and
`decode_json(encoded, expected_type)`. Encoded values must be finite JSON
values. Select the codec on the workflow:

```python
@duron.workflow(codec=application_codec, version="0")
async def flow(ctx: duron.WorkflowContext, key: str) -> str:
    return await ctx.call(fetch, key)
```

The workflow codec handles application values throughout the run, including
effect arguments and results and port and channel payloads. Runtime framing
uses fixed JSON. Effects do not configure serialization.
Bump the workflow version and use a new run ID when the encoding contract
changes incompatibly. Keep the old definition and codec available to finish
existing runs. References include the codec identity. Set `codec_id="orders.v2"`
when codec configuration changes its wire contract; code changes under the same
identity cannot be detected automatically.

The default codec accepts only finite JSON values: `None`, booleans, numbers,
strings, lists, and dictionaries with string keys. It does not reconstruct
dataclasses, tuples, dates, or other Python objects from annotations. See the
[codec reference](../reference/codec.md) for a complete custom codec example.
