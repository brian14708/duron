# API design review

Reviewed 2026-10-16. Scope: every public module, its exported types and returned
objects, authoring decorators, host lifecycle, channels, storage, codecs,
exceptions, examples, and automation. This is a design review, not a complete
audit of replay and storage correctness.

The recommended teaching model has four concepts: **workflow, effect, port,
and run**. Contexts provide operations on those concepts. Definition types,
bound calls, and returned handles should normally be inferred. Prefer static
typing for application contracts; retain runtime checks for persisted data,
ownership, and external protocol boundaries.

Dependency declarations, advisory warnings, and public context protocols are
implemented. The unused cache-size exception has also been removed. The
remaining recommendations below are proposed work.

## Findings, in priority order

### 1. Public types expose too much private runtime machinery

The context boundary is now resolved: `WorkflowContext` and `EffectContext`
are public protocols backed by private runtime implementations. Effect authors
see only idempotency and recovery properties; workflow authors see durable
operations. Construction, invocation identities, and channel binding are
internal. The workflow clock has one public read operation, `now`.

Channel boundaries still need work. `DurableCommands.id` returns the private
`SignalId`, and `EffectHost.control_sender` accepts it. Channel handles expose
`reference`, `for_owner`, and producer tracking. Returned stream/command scopes
expose `close_scope`, although their context managers already own cleanup.

Sources: `src/duron/_api/context.py`,
`src/duron/_runtime/execution/handles.py`,
`src/duron/_runtime/workflow/owned_signal.py`, and
`src/duron/_runtime/workflow/owned_stream.py`.

The remaining channel surfaces make type annotations and autocomplete teach
transport internals to ordinary authors. Keep ownership and cleanup private.
Where transport authors actually need an identity, give that opaque type one
supported home in `duron.execution.remote`.

### 2. Definitions have mutable configuration with inconsistent effects

`Workflow.name`, `version`, `codec`, and `codec_id` are writable. The decorator's
shim captures the original name and codec, while `Workflow.__call__` reads the
current public fields. Mutation can therefore change the run header without
changing the codec used by workflow operations. `Effect` also exposes writable
configuration, including its signature and execution policy.

Sources: `src/duron/_api/workflow.py` (`Workflow`, `workflow`) and
`src/duron/_runtime/execution/effect.py` (`Effect`).

Make definition configuration statically read-only and give execution one
source of configuration. Changing configuration should construct another
definition, as the existing service factory example already does. Avoid adding
a mutable registration phase or a runtime freezing framework. The new
`Workflow.requires` follows the read-only property approach and snapshots its
input sequence at decoration.

### 3. The name-based port contract has a limit static types cannot express

The workflow caches each kind of port by name. Host input and interrupt writers
receive only the backing stream name. Two independently created declarations
can therefore refer to the same runtime channel while claiming different
payload types. A probe using `Output[int]("same")` and `Output[str]("same")`
produced `[1, "unexpected string"]` from the integer reader. Both sends are
individually valid according to their declarations.

Sources: `src/duron/_runtime/workflow/api_context.py` (`emit`, `_input_reader`),
`src/duron/_api/run.py` (`Run.input`, `Run.interrupt`), and
`src/duron/_api/ports.py` (`_NamedPort`).

Prefer sharing one typed declaration between workflow and host, as the examples
already do. Document that independently repeating a string does not establish
type compatibility. A normal Python type checker cannot prove uniqueness of
arbitrary runtime names; full static detection would require a dedicated
analyzer. Do not add runtime schema registries as part of API minimization.
Keep `stream_name` private so applications do not couple to its encoding.
Dependency warnings are advisory and do not make this a checked runtime schema
contract, especially when both conflicting objects are declared.

### 4. The inert bound call unnecessarily has a second lifecycle

`WorkflowCall._launched` makes an otherwise inert argument binding single-use.
Applications must learn the difference between definition, unused bound call,
consumed bound call, and live run. Reusing a binding is rejected even for a
different run ID.

Source: `src/duron/_api/run.py` (`WorkflowCall._launch`).

Prefer a reusable, immutable binding if the runtime does not need exclusive
ownership of its arguments. Put execution state on `Run` and its owning
context. Preserve `run` for result-only execution and `open_run` for interaction:
these entry points solve different lifecycle needs. Keep `WorkflowCall` in
`duron.host` for annotations rather than promoting it to another root concept.
Any removal of single-use behavior should update callers and documentation
together.

### 5. Advanced concepts are mixed into the introductory surface

`duron.execution` combines effect policy, scoped workflow channels, and recovery
inspection descriptors. `StreamParameter` sits there while the corresponding
command parameter descriptors live in `duron.execution.remote`. The getting
started guide goes all the way through storage implementations and transport
authorization.

Sources: `src/duron/execution/__init__.py`,
`src/duron/execution/remote.py`, and `docs/getting-started.md`.

Keep effect policy and ordinary channel handles together. Put parameter
descriptors, posting wrappers, and explicit reporting in the remote layer;
recovery inspection is available through `EffectReporter.read_events`.
Split advanced channel, storage, and transport
guides out of the introductory path. Use one public import for each type and
remove the old export when moving it.

### 6. Several small surfaces can shrink without losing a concept

`OutputReader` and `RequestReader` expose iteration, `next`, and `collect` over
the same cursor. Prefer standard async iteration and `anext` by implementing
the iterator protocol, with list comprehensions for collection. This would
also remove the special `InvalidRunStateError` interpretation of iteration
exhaustion. Preserve independent readers and durable output cursors.

The unused cache-size exception has been removed. Retain exception classes
according to whether applications take different recovery actions, not merely
because different internal conditions exist.

Workflow and effect decorators independently validate context annotations with
different rules. For example, workflow decoration accepts a keyword-only first
context, even though injection is positional; the typed callable contract
already rejects it. Prefer the callable type signatures for authoring mistakes
and reduce duplicate runtime annotation enforcement. Keep schema extraction
needed for serialization separate from user-facing type enforcement.

Sources: `src/duron/_api/run.py` (`_PollingReader`), `src/duron/errors.py`,
`src/duron/_api/workflow.py` (`_validate_context_param`), and
`src/duron/_runtime/execution/definition.py` (`effect_signature`).

## Which concepts should remain?

| Surface | Recommendation | Reason |
| --- | --- | --- |
| Workflow and `WorkflowContext` | Keep as the orchestration concept | One place for replay-aware operations. |
| Effect and `EffectContext` | Keep as the external execution concept | External work needs idempotency, retries, and recovery. |
| `Input` | Keep as a port form | FIFO consumption is a useful typed contract. |
| `Output` | Keep as a port form | A durable event log permits independent readers and reconnection. |
| `Request` | Keep as a port form | It owns correlation, one reply, and replay across host reconnection. |
| `Interrupt` | Keep as an optional port form | It changes control flow within a scope rather than supplying an ordinary value. |
| `Run`, `run`, `open_run` | Keep as one execution concept | A host needs a result-only path and an explicitly owned interactive lifetime. |
| Scoped effect streams | Keep in the advanced API | Producer ownership and channel completion differ from host output logs. |
| Commands and acknowledgement | Keep in the advanced API | Effect delivery and acknowledgement have different recovery semantics from host input consumption. |
| `RetryPolicy` | Keep as optional effect configuration | One cohesive policy covers retries and ambiguous recovery. |
| Storage protocol and built-in stores | Keep in the storage module | Four protocol methods already form a compact integration boundary. |
| Codec protocol | Keep as optional configuration | Encoding is an extension point, not another orchestration concept. |
| Transport reporting types | Keep confined to remote integration | Most applications need only dispatch and `host.execute`. |

In particular, replacing `Request` with an input/output pair makes each
application implement request IDs, response routing, failure reporting, and
recovery. Replacing it with an effect makes the application provide a dispatcher
for interactions such as human approval. Keeping the typed request port is the
smaller application-facing design. Similarly, one generic port with a mode flag
would move distinctions into strings and method validity rules, weakening
static checking.

Use `Input[T]`, `Output[T]`, `Request[Req, Res]`, and `Interrupt[T]` as the
typed port vocabulary. Avoid adding a public base port, dependency object,
service container, request context, or workflow-specific task scheduler.
Ordinary helper functions can accept the existing context; ordinary asyncio
constructs express concurrency.

## Implemented dependency contract

```python
approval = duron.Request[str, bool]("approval")
events = duron.Output[str]("events")


@duron.workflow(requires=(charge, approval, events))
async def checkout(ctx: duron.WorkflowContext, order_id: str) -> bool:
    if not await ctx.request(approval, order_id):
        return False
    await ctx.call(charge, order_id)
    await ctx.emit(events, order_id)
    return True
```

`requires` accepts existing effects and all four port kinds. Its annotations
let the type checker reject unrelated values. Decoration copies the sequence
to a read-only tuple without runtime type validation. `workflow.requires` makes
the boundary inspectable without execution or a separate manifest object.

Using an undeclared object through the context emits a standard `UserWarning`
once per object per execution. Matching uses the actual shared declaration,
not just its name. Omission means an empty declaration. Unused declarations are
allowed for conditional paths. Helpers using the same context get the same
checks. Requests and interrupts are included; clocks, randomness, sleeping,
runtime-created channels, and services closed over inside effects are not
separate dependencies.

This is an advisory check of executed paths. Ordinary type checkers cannot
infer that every object used in an arbitrary async body belongs to a decorator
argument sequence. Achieving that statically would require a dedicated analysis
tool or a substantially more restrictive capability API. Neither is introduced
here. Standard Python warning filters can suppress warnings or promote them to
errors in tests. A directly scheduled coroutine may report an asyncio caller
location, so the warning includes the workflow and dependency names.

Declarations are not a service-health check, a host handler registry, an
injection mechanism, or a security boundary. They are not persisted and do not
alter history compatibility. Replay checks executed paths again, subject to
normal warning filtering.

## Public surface budget and next steps

The root exports 13 names. The other explicit export lists contain 7 host,
12 execution, 14 remote, 5 storage, 3 codec, and 2 typing names. There are also
25 error classes. These counts understate the surface because returned objects
expose additional methods and private types. The dependency change adds one
decorator option and one property, with no new root export or public class.

Address the remaining channel runtime leakage and mutable definition configuration first.
Then simplify the bound-call and reader APIs, remove unused symbols, and
separate advanced documentation. Preserve the four core concepts and typed
port forms. For each intentional break, update callers, exports, examples,
tests, and hidden configuration together, without compatibility aliases.
