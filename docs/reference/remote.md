# `duron.execution.remote` module

`EffectExecutor` is the effect-specific adapter chosen by
`@duron.effect(executor=...)`. Its `dispatch` method decides where an invocation
runs. `EffectHost` belongs to the active run; it validates a request and commits
reports to that run's history. The executor receives the host as an argument,
but does not own the history or its storage lease.

An executor may dispatch to different processes, containers, or services,
including services written in another language. `EffectHost.execute` is the
convenience path for a Python effect implementation. A non-Python service needs
a Python adapter to translate its results into the host's typed reporting API.
Duron does not yet specify a language-neutral worker protocol or SDK.

Remote dispatch adapters carry an opaque `EffectEnvelope` (`bytes`).
`EffectHost.execute` handles the usual decode, invoke, and durable report path
for a registered effect. It executes the Python body in the process where that
method runs and returns a `Receipt`. Lower-level
`EffectHost.decode` and `bind` expose an `ExecutionRequest` and `EffectReporter`
when a transport needs explicit reporting or recovery. `request.context`
provides the read-only `EffectContext` protocol. Binding a context to an
invocation stays internal to `host.execute`. Use `EffectReporter.read_events`
for recovery inspection of effect output streams.
After reconnection, authenticate the envelope before calling
`EffectHost.rebind`. Rebinding requires the executor to implement
`authorize_rebind(envelope)`; otherwise it is rejected.

The following runnable example demonstrates the adapter contract in one
process. The effect selects the executor, which executes the registered
definition through the host.

```python
import asyncio

import duron
from duron.execution.remote import EffectEnvelope, EffectHost
from duron.storage import MemoryStore


class WorkerExecutor:
    async def dispatch(self, envelope: EffectEnvelope, *, host: EffectHost) -> None:
        await host.execute(envelope, effect=double)


@duron.effect(name="jobs.double", version="1", executor=WorkerExecutor())
async def double(ctx: duron.EffectContext, value: int) -> int:
    return value * 2


@duron.workflow(requires=(double,))
async def calculate(ctx: duron.WorkflowContext) -> int:
    return await ctx.call(double, 21)


async def main() -> None:
    store = MemoryStore()
    for _ in range(2):
        assert await duron.run(calculate(), store, run_id="double") == 42


asyncio.run(main())
```

`host.decode` and `host.execute` validate against the active run's registration,
including Python function identity. A separately constructed effect with the
same name, version, and signature is not a substitute. In particular, a fresh
closure with a worker-local service cannot be passed to the run owner's host
as a replacement implementation.

For another process, an adapter must decode and bind on the run owner, translate
the request into its own transport, execute application work remotely, and
forward typed outcomes through the bound reporter. Duron does not ship an RPC
proxy, a worker service, or a public detached Python-effect runner. Forwarding
`host.execute` to the run owner executes the body there. Treat envelopes as
opaque capabilities and authenticate worker traffic; possession of serialized
bytes alone is not application authorization.

`EffectHost.execute` derives a stable report key from the invocation number.
That makes duplicate outcome reports idempotent, not duplicate execution of the
effect body. The body still needs external idempotency.

After reconnection, `await host.rebind(request)` returns a new `EffectReporter`,
not a new request or execution context. Rebinding calls the configured
executor's optional `async authorize_rebind(envelope, /)` hook; the hook must
raise if the request is unauthorized. Without the hook, rebinding is rejected.
Use the returned reporter for reporting and recovery inspection.

Lower-level `complete`, `fail`, and `cancel` reports require an explicit
`idempotency_key`. A repeated identical post returns a duplicate receipt;
changing the action or payload under that key raises `PostConflictError`.
`Receipt.offset` identifies the committed record, and `Receipt.duplicate`
indicates a previously committed post.

## ::: duron.execution.remote
