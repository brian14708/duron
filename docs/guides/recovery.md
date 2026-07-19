# Replay, recovery, and application services

Duron recovers a workflow by replaying recorded operations and continuing where
it left off. Workflow control flow must stay deterministic, and recorded effect
outcomes must be reused. The sections below explain how to write workflows and
configure their services for recovery.

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

## Handle interrupted effects

When replay finds an effect call without a final outcome, Duron reports the
interrupted effect and does not invoke it again. A scheduled run becomes
`BLOCKED`; start a new run with a new ID after resolving any external work.
Completed effects replay their recorded outcomes. Success, failure, and
cancellation are each final.

Result validation or encoding failures that raise `TypeError` or `ValueError`
are recorded as application failures. Duron does not retry the effect.

For external mutations, pass stable application operation IDs as effect
arguments so the external service can identify work whose outcome was lost.

## Scheduler failures and cancellation

Transient storage failures, including during replay or cancellation, use the
scheduler's bounded infrastructure retry policy. These retries do not make an
effect with a missing outcome safe to invoke again.

If cancellation encounters incompatible or corrupt history, the scheduler
records a failure for that run without rewriting its history. Other runs
continue. Use `run.status()` for scheduling diagnostics and `run.result()` to
observe the error. See [runtime failures](../reference/errors.md#runtime-failures).

## Supply typed application services

An effect can close over an application service. Construct the definition with
the service it will use:

```python
from typing import Protocol

import duron
import duron.execution


class OrderClient(Protocol):
    async def fetch(self, key: str) -> str: ...


def make_fetch(
    client: OrderClient, executor: duron.execution.Executor | None = None
) -> duron.Effect[[str], str]:
    @duron.effect(name="orders.fetch", executor=executor)
    async def fetch(_ctx: duron.EffectContext, key: str) -> str:
        return await client.fetch(key)

    return fetch
```

The application that constructs the effect also manages the client's lifetime.
Omit `executor` to run the effect locally. Pass a custom executor to
`make_fetch(client, executor)` to choose a different execution adapter for this
effect. Each effect definition selects its own executor. Register the workflow
that calls it with `SQLiteScheduler(path, workflows=[...])`.

`AsyncExecutor` runs the registered Python function by default.
A remote executor uses `invocation.arguments`, `metadata`, and `port(...)`, then
returns its result or raises `ApplicationFailure(error)`. See the
[execution reference](../reference/execution.md).

## Use a custom codec

Implement `duron.codec.Codec.encode_json(value, annotated_type)` and
`decode_json(encoded, expected_type)`. Encoded values must be finite JSON
values. Select your codec instance with
`@duron.workflow(codec=application_codec)`.

The workflow codec handles application values throughout the run, including
workflow and effect arguments and results and port payloads. Effects use the
calling workflow's codec.

The default codec accepts only finite JSON values: `None`, booleans, numbers,
strings, lists, and dictionaries with string keys. It does not reconstruct
dataclasses, tuples, dates, or other Python objects from annotations. See the
[codec reference](../reference/codec.md) for a complete custom codec example.

## Change definitions and codecs

Names identify workflow and effect definitions; Duron does not fingerprint
their code. Keep changes compatible with existing runs, or choose a new name
and run ID for incompatible workflows. Keep old definitions and codecs
available to finish existing runs. Evolve `decode_json` to accept previously
persisted values, using in-band version tags when needed.
