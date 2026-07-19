# Durable execution engines and lessons for Duron

Research date: 2026-10-15. Duron source references checked 2026-10-16.
This research includes proposals; the [guide](../getting-started.md) and
[API reference](../reference/duron.md) describe the supported application API.

Duron is an embeddable, typed Python async runtime for durable workflows with
explicit effects, interactive ports, and pluggable history storage.
The most useful lessons from other engines concern operating that model over
time: recovery ownership, code deployment, growing histories, and the behavior
of interactive clients during failures.

An external orchestration server, a workflow DSL, a general agent framework, and
a mandatory database service are not prerequisites for those improvements.

## Scope and evidence

This is a survey of the major execution models and relevant implementations,
not a claim to enumerate every workflow package. It covers direct durable-code
competitors, managed orchestration, stateful runtimes, and adjacent job, data,
and agent systems. Adjacent systems are useful references without being
equivalent substitutes.

The comparison uses Duron's source, documentation, and existing tests, plus
primary vendor documentation and official repositories. Links accompany each
engine. Public preview and unreleased source are identified where material.
Documentation establishes intended behavior; it does not prove a deployed
engine implements every advertised guarantee. Competitor runtimes were not
installed or benchmarked. No performance, cost, or production-reliability
ranking is inferred from marketing claims.

Sources change. In particular, current Temporal documentation includes Workflow
Streams in public preview; Golem `next` documentation and Vercel's v5 source
describe behavior that must be checked against a selected release.

## What Duron actually has

The baseline matters because several common recommendations are already done.

| Area | Current behavior | Evidence |
| --- | --- | --- |
| Authoring | Decorated Python async workflows, explicit decorated effects, and typed input, output, request, interrupt, stream, and command ports. | [documentation](../index.md), [guide](../getting-started.md) |
| Replay | Append-only per-run history; completed operations matched by deterministic position and recorded identities. This is distinct from content-based result caching. | [session replay](https://github.com/brian14708/duron/blob/main/src/duron/_runtime/workflow/session.py) |
| Execution | A custom deterministic workflow event loop hosted inside asyncio; effects execute outside the replayed orchestration. | [event loop](https://github.com/brian14708/duron/blob/main/src/duron/_runtime/loop.py), [session](https://github.com/brian14708/duron/blob/main/src/duron/_runtime/workflow/session.py) |
| Identity | Explicit `run_id`; workflow name/version and normalized inputs checked on reuse; effect descriptors include signature, policy, schema, and codec identity. | [invocation](https://github.com/brian14708/duron/blob/main/src/duron/_api/run.py), [registration](https://github.com/brian14708/duron/blob/main/src/duron/_runtime/execution/executor.py) |
| Effect delivery | External actions can repeat after interruption. Stable `EffectContext.idempotency_key` identifies the logical execution. | [execution models](https://github.com/brian14708/duron/blob/main/src/duron/_runtime/execution/models.py), [guide](../getting-started.md) |
| Retry | Logical failures and ambiguous crash recoveries have separate budgets. Retry deadlines are persisted. Backoff, exact-type retry selection, and unknown outcomes already exist. | [attempt state](https://github.com/brian14708/duron/blob/main/src/duron/_runtime/workflow/attempts.py), [attempt tests](https://github.com/brian14708/duron/blob/main/tests/test_attempts.py) |
| Time | `ctx.now`, `ctx.random`, and durable `ctx.sleep`. Differences between `now` values measure elapsed time; sleep preserves the original deadline across recovery. | [workflow context](https://github.com/brian14708/duron/blob/main/src/duron/_api/context.py) |
| Ownership | Per-run fencing tokens; a new acquisition immediately fences earlier writers. Atomic append batches and ambiguous-commit recovery. | [backend contract](https://github.com/brian14708/duron/blob/main/src/duron/_runtime/storage/backend.py), [append writer](https://github.com/brian14708/duron/blob/main/src/duron/_runtime/workflow/append.py) |
| Local persistence | SQLite WAL with `synchronous=FULL`; `MemoryStore` and application implementations also satisfy the history store protocol. | [backends](https://github.com/brian14708/duron/blob/main/src/duron/_runtime/storage/backends.py), [storage tests](https://github.com/brian14708/duron/blob/main/tests/test_log.py) |
| Streaming | Effect-owned streams, durable sends, independent readers, paged reads, bounded decoded prefetch, semantic deduplication keys, explicit EOF distinct from final effect result. | [owned streams](https://github.com/brian14708/duron/blob/main/src/duron/_runtime/workflow/owned_stream.py), [stream tests](https://github.com/brian14708/duron/blob/main/tests/test_core.py) |
| Commands | Effect-owned FIFO signal delivery, one unacknowledged message, durable acknowledgements, redelivery after failure, scoped closure. Workflow interrupt ports are a separate mechanism. | [owned signals](https://github.com/brian14708/duron/blob/main/src/duron/_runtime/workflow/owned_signal.py), [delivery state](https://github.com/brian14708/duron/blob/main/src/duron/_runtime/workflow/session.py) |
| External execution | Transport-neutral dispatch, explicit completion reports, authenticated rebinding, stale capability fencing, and durable duplicate receipts. | [execution API](https://github.com/brian14708/duron/blob/main/src/duron/execution/__init__.py), [external tests](https://github.com/brian14708/duron/blob/main/tests/test_core.py) |
| Concurrency | Application code uses async tasks for bounded fan-out; append batches and stream prefetch are bounded separately. | [guide](../getting-started.md), [workflow context](https://github.com/brian14708/duron/blob/main/src/duron/_api/context.py) |
| History compatibility | Format version 1; version 0 and other unsupported formats are rejected before dispatch, without overwriting or silently migrating history. | [documentation](../index.md), session validation |

Important limits follow from the existing design:

- Recoverable state does not start a replacement process. The application must
  reopen the run after a crash. The backend protocol does not list pending runs,
  schedule activation, or deliver a timer wakeup to a stopped host.
- A fencing token prevents stale history writes. It cannot retract an HTTP
  request, stop an external worker, or roll back a payment already submitted.
- Paged reads and bounded prefetch do not bound total persisted history. Current
  recovery replays the run's history; a long conversation continues accumulating
  stream, receipt, and attempt entries.
- Application-level bounded fan-out is not a cluster-wide tenant quota or API
  rate limit.
- Definition identity detects specific mismatches. It does not prove that an
  arbitrary edit to a Python function body is replay-compatible.
- Durable publication and a consumer's business processing are separate events.
  Effect signal acknowledgements have an explicit protocol; arbitrary output
  consumers do not automatically acquire exactly-once business processing.

## The execution models to compare

| Model | Typical systems | Durable boundary | Main lesson |
| --- | --- | --- | --- |
| Deterministic workflow replay | Duron, Temporal, Cadence, Durable Task, Azure Durable Functions, Dapr Workflow | Recorded operations rebuild orchestration state. | Determinism, deployment compatibility, history lifecycle. |
| Journaled handlers and durable calls | Restate, Resonate | Recorded context operations, results, messages, and promises. | Invocation identity, cancellation, durable composition. |
| Database-backed steps and jobs | DBOS, Absurd | Stored step outcomes and transactional workflow/job state. | Embedded operation, SQL boundaries, recovery discovery. |
| Service-managed step execution | Inngest, Cloudflare Workflows, Lambda durable functions | Persisted results and service-driven resumption. | Idle execution, ingress, quotas, operational visibility. |
| Transformed code and portable backends | Vercel Workflow DevKit | Workflow/step events through a backend abstraction. | Local development, hooks, resumable streams. |
| Process checkpointing | Trigger.dev | Process snapshots at supported waits, plus durable task orchestration. | Releasing compute and separating snapshot resume from failure retry. |
| Durable virtualized runtime | Golem | Host interactions and runtime state under a controlled execution environment. | Precise external boundaries and snapshot semantics. |
| Declarative state machine | Step Functions, Google Workflows, Conductor, LittleHorse | Engine-managed state transitions and worker results. | Inspection, callback lifecycle, compensation, policy visibility. |
| Job or data orchestration | Oban, Celery, BullMQ, Airflow, Dagster, Prefect, Argo, Flyte | Job/task/node state and artifacts. | Admission, operations, lineage, recovery UX. |
| Agent state graphs | LangGraph, Mastra | Graph checkpoints, step state, and user-controlled resume boundaries. | Interaction ergonomics and separating application memory from execution state. |

These categories describe mechanisms, not mutually exclusive product labels.
For example, DBOS has queues and streams, Hatchet has durable replayed parent
tasks, and Dapr Workflow is a code-replay engine rather than a workflow DSL.

## The closest comparisons

### DBOS

DBOS is the most direct challenge to "embedded durable Python" positioning.
Its [architecture](https://docs.dbos.dev/architecture) explicitly describes an
open-source library using Postgres, without a separate orchestration server.
Its optional Conductor coordinates distributed recovery and provides operations
tooling outside the workflow orchestration path. This is DBOS Conductor,
unrelated to Netflix/Orkes Conductor.

DBOS also has [durable append-only streams](https://docs.dbos.dev/ai/streaming)
with Python writers, readers, and explicit or automatic closure. Its database
integration makes transaction boundaries particularly worth studying.

Duron already shares the explicit effect/step boundary and embedded host model.
Learn how DBOS discovers abandoned work, separates queues from execution, and
adds operational tools without forcing every workflow call through a control
plane. A transaction that includes both an application mutation and durable
progress can offer a stronger local guarantee than an arbitrary HTTP effect.
That guarantee requires the same actual transaction; "both use SQL" is
insufficient.

Keep Duron's backend choice and SQLite deployment. Consider database-specific
transaction or outbox integrations as optional adapters, not an assumption in
the core backend protocol. Compare stream recovery experimentally before
claiming either implementation handles regenerated output more safely.

### Absurd

Absurd is a second close reference for minimal infrastructure. Its
[concepts](https://github.com/earendil-works/absurd/blob/main/docs/concepts.md)
describe a Postgres implementation using tables and stored procedures, pull
workers, named checkpoints, leases, durable sleep, and events. A task can be
reclaimed after its lease expires, and the documentation acknowledges briefly
overlapping executions.

Its [code-change guide](https://github.com/earendil-works/absurd/blob/main/docs/patterns/living-with-code-changes.md)
warns that changing a step name can repeat an action such as a payment. This
illustrates why stable checkpoint identity is a correctness concern, not merely
an API naming convention.

Borrow the small operational model, explicit cleanup rules, and honest account
of overlap. Duron already has fencing and stronger descriptor checks worth
keeping. Named checkpoint lookup is a different replay model; importing it
would weaken Duron's positional contract unless designed as an intentional
replacement. These findings use current `main` documentation, not a pinned
release audit.

### Resonate

The [Python SDK](https://github.com/resonatehq/resonate-sdk-py/blob/main/README.md)
provides native async functions, durable promises, `ctx.run`, `ctx.sleep`,
function registration, and remote composition through `ctx.rpc`. Its emphasis
on structured concurrency is particularly relevant to effect ownership.

Resonate can use its server, while the
[Postgres implementation](https://github.com/resonatehq/resonate-pg/blob/main/README.md)
implements the protocol in SQL, with `pg_cron` driving timers. The latter is a
concrete example of moving activation responsibility out of a sleeping
application process. Its cleanup discussion also makes an important connection:
deleting an identity can delete the ability to deduplicate later reuse.

Borrow durable composition and explicit child lifetimes. Duron's scoped
producer/receiver ownership already follows that direction. Keep transport
routing and wakeup services optional. Any future garbage collection needs an
explicit idempotency horizon. Current-main source describes these capabilities;
deployment requirements must be checked for the selected version.

### Vercel Workflow DevKit

The [official SDK source](https://github.com/vercel/workflow/blob/main/packages/workflow/README.md)
describes durable JavaScript/TypeScript functions, step retries, suspension,
local development, and backend implementations called Worlds. Postgres and
custom backends make its portability more relevant than a hosted-only product
comparison suggests. Its transformed workflow/step execution differs from
Duron's Python event-loop approach.

The [v5 streaming documentation](https://github.com/vercel/workflow/blob/main/docs/content/docs/v5/foundations/streaming.mdx)
describes durable resumable streams, reconnection positions, serializable stream
references, and backend-specific stream persistence. It also discusses producer
completion separately from errors that occur later while consuming a stream.
[Hooks](https://github.com/vercel/workflow/blob/main/docs/content/docs/v5/foundations/hooks.mdx)
provide typed, scoped external interaction.

Borrow the clarity of reconnect contracts, local inspection, and typed external
hooks. Duron's owned streams, signals, and requests already provide the core
interaction mechanisms. The transferable work is an application transport
contract and good failure examples. A JavaScript compiler transformation or a
Vercel deployment requirement would change Duron's thesis. The cited v5 source
is version-specific and should not be generalized to every released SDK.

### Temporal

Temporal's [workflow definition](https://docs.temporal.io/workflow-definition)
and [event history](https://docs.temporal.io/encyclopedia/event-history)
are the main references for replay discipline. A durable service owns history
and dispatch; SDK workers replay workflow code and execute activities. Activity
idempotency remains necessary across the external-effect crash gap.

Its strongest lessons for Duron are
[Continue-As-New](https://docs.temporal.io/workflow-execution/continue-as-new),
[worker versioning](https://docs.temporal.io/worker-versioning),
[Python replay/testing tools](https://docs.temporal.io/develop/python/testing-suite),
and [failure detection](https://docs.temporal.io/encyclopedia/detecting-activity-failures).
Duron already has timers, stable execution identities, retries, and version
checks. It needs to evaluate the lifecycle and diagnostics around them, rather
than add their basic mechanisms again.

Current [Workflow Streams](https://docs.temporal.io/workflow-streams) are a
**public preview** supporting Python and other SDKs. They use workflow-hosted
stream state, batched publishing, resumable subscribers, and publisher sequence
deduplication. The docs explicitly distinguish publish retries from activity
retries: a new activity attempt is a new publisher, so subscribers can see
partial output followed by a new attempt's complete output. Truncation does not
remove the underlying history; Continue-As-New is a separate operation.

Borrow that level of guarantee specificity. Keep Duron's embedded execution,
native stream EOF, and explicit owner lifetime. A mandatory Temporal-like
cluster is unnecessary. Do not claim Temporal lacks streaming or that its
exactly-once publishing means exactly-once external work.

### Restate

Restate journals handler operations and provides durable service calls, keyed
virtual objects, workflow promises, and timers. Start with its
[key concepts](https://docs.restate.dev/foundations/key-concepts) and
[Python durable steps](https://docs.restate.dev/develop/python/durable-steps).
The server owns durable invocation progress while application endpoints host
handler code.

Its [session pattern](https://docs.restate.dev/ai/patterns/sessions) uses keyed
objects to serialize writes. The
[interrupt/regenerate pattern](https://docs.restate.dev/ai/patterns/interrupt-regenerate)
discusses cancellation at durable awaits, propagation to nested work, and
durable cleanup. These are useful race scenarios for Duron's existing
interruption and ownership semantics.

The fetched [streaming-response guide](https://docs.restate.dev/ai/patterns/streaming-responses)
uses an extra TypeScript pubsub component and documents limitations on Python
and token streaming. Treat these as documentation-date limitations, not a
permanent product boundary.

Borrow explicit cancellation observation, cleanup behavior, and invocation
identity. Keep Duron's typed Python channels and transport-neutral executor.
Turning every workflow into a network handler or globally keyed actor would
change the authoring and deployment model.

### Trigger.dev

Trigger.dev combines task orchestration with process checkpointing. Its
[architecture](https://trigger.dev/docs/how-it-works) describes CRIU snapshots
at supported waits so compute can be released. Snapshot resumption preserves
process state. A failed task retry is different: it starts task execution again,
with keyed child-task reuse. Neither mechanism makes arbitrary external effects
transactional.

Its [Sessions](https://trigger.dev/docs/ai-chat/sessions) are a particularly
strong interaction reference. They provide a durable input/output stream pair,
a stable external identity spanning runs, idempotent creation, SSE reconnection,
stream trimming, closure, and separate read/write authorization. The
[chat overview](https://trigger.dev/docs/ai-chat/overview) includes steering,
approvals, stopping, recovery, and version handoff.

Borrow the separation of conversation identity, run identity, and execution
attempt. A client connection should survive a replaced producer without having
to understand the worker lifecycle. Duron can support that through host-side
session and transport adapters. CRIU, managed process hosting, and an opinionated
chat-agent API would be major changes to its thesis.

### Inngest

Inngest's [execution concepts](https://www.inngest.com/docs/durable-execution/concepts)
combine triggers, persisted step results, replay, waits, and flow control. Waits
release compute and do not count toward function concurrency. Its
[step documentation](https://www.inngest.com/docs/durable-execution/primitives/step-run)
recognizes the external-effect ambiguity and recommends duplicate-safe work.

Borrow the operational distinction between concurrency, throttling, rate
limits, debounce, and scheduling. They solve different overload problems.
Application-level bounded fan-out does not cover all of these policies.

Keep these controls in the host or dispatch adapter unless replay semantics
require a durable decision. Its service-driven trigger model and named steps
are alternatives to Duron's explicit invocation and positional replay, not
automatic improvements to them.

### Hatchet

Hatchet's [durable execution](https://docs.hatchet.run/v1/durable-execution)
includes checkpointed parent tasks, child tasks, and durable waits, beyond
ordinary queued jobs. Its worker, admission, and visibility tools are useful
operational references.

Its current [embedded mode](https://docs.hatchet.run/v1/embedded) removes manual
infrastructure setup, but Python and TypeScript download and launch a Go
sidecar, which provisions embedded Postgres by default. This is different from
embedding a Python runtime with standard-library SQLite, even if both are
described as embedded.

Borrow a realistic local test experience and explain queued versus active work.
Duron already supports external dispatch, so queue implementations can live
behind that boundary. Do not add a sidecar or queue server merely to match the
feature list. "At-least-once job delivery" alone also understates Hatchet's
current checkpointed workflow capabilities.

### Cadence

Cadence shares the history-replay model, with workflow workers and separate
activities. Its [activity documentation](https://cadenceworkflow.io/docs/concepts/activities)
is useful for separating schedule-to-start, start-to-close, schedule-to-close,
and heartbeat timeouts. Heartbeat details can carry application progress into
the next attempt; heartbeat interaction also participates in cancellation.

Borrow a precise liveness vocabulary for external Duron effects. A stream
emitting application tokens is not automatically a worker heartbeat, and a
quiet approval wait is not automatically a stalled computation. Duron's
external completion API already covers asynchronous reporting; the open
question is detecting and exposing stuck work. Keep queue and heartbeat policy
in optional worker integration where possible.

### Azure Durable Functions and Durable Task

[Azure Durable Functions](https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-overview)
uses event-sourced orchestration with activities, timers, external events, and
entities. The [code constraints](https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-code-constraints)
explicitly require Python orchestrators to use synchronous generators and
`yield`, rather than native `async`/`await`. Duron's native async authoring is a
meaningful distinction here.

Borrow replay-aware logging, clear
[external-event delivery and deduplication](https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-external-events),
and explicit [storage-provider behavior](https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-storage-providers).
Backend atomicity can affect delivery guarantees. Duron already states atomic
batches and durable signal acknowledgement; a backend conformance suite should
make those requirements executable for third-party implementations.

The [Durable Task repository](https://github.com/Azure/durabletask) distinguishes
the community-maintained DTFx library from modern Durable Task SDKs and the
managed Scheduler. Do not treat the entire family as one deployment model.
Its pluggable providers are relevant; SDK and service proliferation need not
be copied.

### Dapr Workflow

[Dapr Workflow](https://docs.dapr.io/developing-applications/building-blocks/workflow/)
is a code-first replay runtime built on Durable Task, with its engine in the
Dapr sidecar and application code connected over gRPC. It supports activity
and child workflows, durable timers, external events, and management APIs.

Borrow explicit parent/child termination behavior and a small management
contract: start, inspect, pause/resume where supported, terminate, and purge.
The interesting part for Duron is what each command means durably, especially
when an external effect continues running. A mandatory Dapr sidecar or actor
subsystem would change the deployment thesis.

### AWS Lambda durable functions

[Lambda durable functions](https://docs.aws.amazon.com/lambda/latest/dg/durable-functions.html)
are a direct code-first comparator distinct from Step Functions. The current
overview describes checkpointed steps, replay from the start while skipping
completed work, waits without compute charges, and executions up to one year.

Borrow the distinction between recording a timer and arranging for execution
to resume when it expires. Duron already implements the first through durable
sleep; an application host or optional supervisor must provide the second
when no process is running.

Keep the runtime portable. Managed activation, IAM integration, quotas, and
cloud billing are properties of the Lambda service, not consequences of a
checkpoint API that Duron can reproduce in a local library alone.

### Cloudflare Workflows

[Cloudflare Workflows](https://developers.cloudflare.com/workflows/) provides
durable steps, waits, retries, and events in the Workers environment. The
[API reference](https://developers.cloudflare.com/workflows/build/workers-api/)
documents step result constraints and supported serialization, including
specific stream-result behavior in current JavaScript Workflows.

Borrow explicit payload boundaries and useful failure messages when a result
cannot be persisted. Large binary results often belong in object storage with
a durable reference, rather than inline in every replay record.

Duron's codec/schema identities already make serialization explicit. Keep
Python and backend portability. Do not assume that a serializable binary stream
result implies the same ownership, streaming progress, or acknowledgement
contract as Duron's channels.

### Golem

Golem executes durable components in a controlled WebAssembly runtime. Its
[durable-execution source](https://github.com/golemcloud/golem/blob/main/docs/src/content/next/concepts/durable-execution.mdx)
describes journaling host interactions and results, replaying ordinary
computation, and optional snapshots. The external HTTP crash gap still requires
idempotency even when runtime interception makes that key automatic.

Its [durable-streams source](https://github.com/golemcloud/golem/blob/main/docs/src/content/next/develop/durable-streams.mdx)
is a useful protocol checklist: producer identity, epoch and sequence,
uncertain appends, fencing, atomic append-and-close, cursor retention, and the
rule that errors must not masquerade as EOF. These are current `next` docs;
check the selected release before relying on those APIs.

Borrow the guarantee table by boundary and the treatment of uncertain output.
Duron already has several corresponding invariants in receipts, fencing, and
owned-channel finalization. Transparent system-call interception, WASM
components, and serialized runtime snapshots would replace its execution
model. A snapshot for Duron should never mean pickling a live Python coroutine.

## Managed state machines and worker orchestration

| Engine | What it persists and operates | What Duron can learn | Boundary to keep |
| --- | --- | --- | --- |
| [AWS Step Functions](https://docs.aws.amazon.com/step-functions/latest/dg/choosing-workflow-type.html) | Managed Amazon States Language execution; Standard and Express have different guarantees and limits. | Explicit execution profiles, audit history, [callback lifecycle](https://docs.aws.amazon.com/step-functions/latest/dg/connect-to-resource.html), [retry jitter and redrive semantics](https://docs.aws.amazon.com/step-functions/latest/dg/concepts-error-handling.html). | Keep Python authoring. Standard's exactly-once state execution is not a blanket external-effect guarantee; configured retries can repeat a task. |
| [Google Cloud Workflows](https://docs.cloud.google.com/workflows/docs/overview) | Managed YAML/JSON workflows with service calls, branching, waits, and connectors. | Clear authentication and callback examples; explain which payloads and errors cross durable boundaries. | Cloud connectors and a declarative language belong outside Duron's execution core. |
| [Conductor OSS](https://github.com/conductor-oss/conductor), [Orkes](https://orkes.io/content/docs) | Versioned workflow definitions, persisted task state, queues, worker polling, management. | Inspectable pending work, explicit retries/timeouts, compensation, and operational intervention. | A central workflow catalog and DSL would change the Python-library thesis. Verify OSS versus commercial capabilities separately. |
| [LittleHorse](https://littlehorse.io/) | Distributed workflow/task state machines and worker execution. | Human-task lifecycle and separation of workflow progress from task-worker availability. | Useful architectural reference, not evidence Duron needs another distributed control plane. Detailed guarantees need a version-specific follow-up. |

Step Functions is especially useful as a policy reference. Duron's existing
backoff lacks a jitter field in the inspected `RetryPolicy`. If jitter becomes
necessary, choose the deadline once and persist it; replay must not reroll it
or consume a different random sequence. This is a narrow extension to an
existing mechanism, not a new retry subsystem.

Compensation also belongs at a precise boundary. A saga is a sequence of
durable compensating actions, not a distributed rollback. Start with a recipe
using existing effects and stable keys. A dedicated saga API is justified only
if repeated use exposes an invariant that the existing API cannot express.

## Adjacent systems worth learning from

These systems solve related problems. Their rows are narrower than the direct
comparisons and do not claim identical replay or delivery semantics.

| System | Relevant execution model | Useful lesson for Duron | Avoid importing |
| --- | --- | --- | --- |
| [Prefect](https://docs.prefect.io/v3/concepts/flows) | Python flows/tasks, state transitions, retries, caching, deployments, optional server/cloud operations. | A Python-friendly local-to-operated experience and readable task failure state. | Treating cache identity as authoritative replay identity, or requiring a server for local execution. |
| [Dagster](https://dagster.io/docs) | Asset and partition orchestration with event logs and lineage. | Show which durable inputs and outputs explain a result; make historical inspection useful. | An asset graph as the core workflow abstraction. |
| [Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/overview.html) | Scheduled DAG/task execution backed by metadata and executors. | Pools, retry inspection, scheduling status, and operator intervention. | Its mostly static batch DAG model. The [project explicitly distinguishes itself from streaming](https://github.com/apache/airflow). |
| [Argo Workflows](https://argo-workflows.readthedocs.io/en/latest/) | Kubernetes container DAGs and steps. | Resource admission, artifacts, and explicit task isolation. | Requiring Kubernetes or a pod per effect. |
| [Flyte](https://github.com/flyteorg/flyte) | Typed distributed task/data workflows. | Type-aware task boundaries and separating large artifacts from control records. | A containerized data platform or assuming cached task outputs equal positional replay. |
| [Metaflow](https://docs.metaflow.org/introduction/what-is-metaflow) | Python data/science flows with artifacts, metadata, and remote compute. | Local development that preserves a useful trail when execution moves remotely. | Versioning whole scientific artifacts as the default runtime responsibility. |
| [Kestra](https://kestra.io/docs) | Declarative event-driven orchestration and task plugins. | Trigger configuration and explaining operational state to users. | A broad plugin platform and configuration DSL in the core. |
| [Windmill](https://www.windmill.dev/docs/flows/flow_editor) | Persisted script jobs and OpenFlow workflows, approvals, worker platform. | Human approval, cancellation, typed forms, and webhook integration examples. | Building an internal-tool/UI platform. |
| [n8n](https://docs.n8n.io/flow-logic/waiting/) | Visual workflow automation with persisted waits and resumes. | Approval and webhook resume ergonomics, especially correlation and expiration. | A connector catalog and visual workflow authoring model. |
| [Oban](https://oban.hexdocs.pm/Oban.html) | Database-backed Elixir jobs; separate Pro workflow features. | Atomic application update plus enqueue, uniqueness windows, queue limits, and graceful rescue. | Equating job uniqueness with exactly-once external actions. Supported databases vary by release; current-main claims must not be projected onto stable. |
| [BullMQ](https://docs.bullmq.io/guide/jobs/stalled) | Redis jobs, locks, stalled-job recovery, flows, and rate controls. | Make lock loss and duplicate execution visible; isolate admission from execution. | A Redis dependency for replay or the assumption that a lock prevents external duplicate work. |
| [Celery](https://docs.celeryq.dev/en/stable/userguide/tasks.html) | Broker/worker tasks, acknowledgements, retries, chains, groups, and chords. | Document acknowledgement timing and worker-loss behavior; route effects through adapters. | Treating task result storage or Canvas graphs as durable coroutine replay. |
| [Dramatiq](https://dramatiq.io/guide.html) | Broker-backed Python actors with retries and middleware. | Small worker APIs and explicit retry middleware boundaries. | A broker requirement for in-process workflow use. |
| [RQ](https://python-rq.org/docs/) | Redis-backed Python jobs and workers. | Low-friction job inspection and manual retry operations. | Replacing workflow replay with rerunning whole jobs. |
| [Taskiq](https://taskiq-python.github.io/) | Async Python task queue with brokers and middleware. | An async dispatch adapter can preserve host integration. | Confusing async task submission with durable orchestration state. |
| [Sidekiq](https://github.com/sidekiq/sidekiq/wiki/Best-Practices) | Redis-backed Ruby jobs and retries. | Its explicit at-least-once/idempotency guidance and conservative job payloads. | Queue throughput as a substitute for correctness at external boundaries. |
| [LangGraph](https://docs.langchain.com/oss/python/langgraph/persistence) | Thread-scoped graph checkpoints plus separate application stores. | Separate execution checkpoints from long-term conversation memory; make interrupt/resume and state inspection understandable. | Requiring graph authoring or silently treating arbitrary node code as an exactly-once effect. |
| [Mastra](https://mastra.ai/docs/workflows/suspend-and-resume) | Workflow step snapshots, explicit suspend/resume, typed resume data. | Make human-input schema and resume location visible. | An agent/application framework or treating step-state snapshots as Python stack snapshots. |
| [Pydantic AI durable execution](https://ai.pydantic.dev/durable_execution/overview/) | Integrations that wrap agent work with an underlying durable engine. | A small, typed integration can make Duron useful to an existing agent framework. Execution durability and saved chat history are different responsibilities. | Building another model/tool framework. This is an integration reference, not an independent competing engine. |

Queue libraries have an important common lesson: job delivery, workflow replay,
and business idempotency are separate contracts. Data orchestrators add another
distinction: rerunning a partition or retrieving a cached artifact is not the
same operation as resuming the exact durable history of an interactive run.

## What the guarantees mean

An engine's headline "exactly once" is not useful until the object and boundary
are named. Use the following questions in future comparisons and Duron docs.

| Boundary | What can be guaranteed | What still needs application help |
| --- | --- | --- |
| Completed durable operation | Replay can return the recorded result without executing the body again. | A crash before the completion record can leave an ambiguous external outcome. |
| External payment, email, or tool call | A stable execution key can support deduplication at the external service. | The recipient must honor the key, or mutation and progress must share a transaction. |
| Stream post | Duron's semantic post key can identify one persisted action and detect conflicting reuse. | A new model generation may produce different content and cannot automatically reuse that action's identity. |
| Signal command | Committed acknowledgement advances the cursor; an unacknowledged command redelivers. | Business work between receive and acknowledgement can already have succeeded. |
| Ownership replacement | A stale owner can be fenced from writing history or using old reporting authority. | It may still hold sockets, run CPU work, or act on an external system. |
| Output reader | A cursor can resume persisted output from a defined location. | The reader can fail after acting but before saving its own cursor. |
| Durable sleep | The original deadline can survive recovery. | Some live process or service must activate the run afterward. |
| History cleanup | Storage can be reclaimed under a declared retention policy. | Old cursors and duplicate keys need explicit expiration semantics. |

The stable idempotency identity is normally the logical external operation.
Using a new key for every recovery attempt would authorize a second payment.
Conversely, two genuinely different actions must not reuse one key just because
they share arguments or occupy similar UI positions.

## The interactive failure cases worth studying

### A token stream fails halfway through a model generation

Suppose the consumer has received `A`, `B`, `C`. The model connection then dies
before the effect's result is committed. A fresh request may produce `A`, `D`,
`E`; language model generation is not guaranteed to reproduce the earlier
prefix.

Duron's existing semantic keys can deduplicate identical persisted actions and
reject different content under a reused key. That is a correctness property.
It is not a promise that a fresh generation can safely continue at token 4.

Applications need an explicit choice: reattach to a provider-supported response,
resume from a provider cursor, expose a new generation attempt, or terminate the
old generation and start another. Offset alone is a transport position, not a
semantic identity for newly generated text. Never silently adopt a regenerated
prefix to make a retry look successful.

Temporal's preview explicitly exposes output from multiple activity attempts.
Trigger Sessions separates the stable session from its runs. DBOS supplies
durable streams, but the cited introduction does not settle every partial-retry
question. Duron can distinguish itself through a precise Python contract and
examples that show these cases, without claiming that streaming is unique.

### A user presses stop while an external effect finishes

There are several distinct moments: the stop command is persisted, the runtime
observes it, local code receives cancellation, an external worker notices, and
an external side effect stops or commits. They cannot be treated as one atomic
instant.

Use Restate's interruption behavior, Cadence's heartbeat cancellation, and
Duron's channel implementation to specify the outcomes of each ordering. Duron
already documents that closing a scope cannot stop external work. A durable
signal command to an effect also does not implicitly interrupt Python code.
Keep that distinction visible when adding transport/UI examples.

### A client disconnects after receiving output

The workflow should not depend on the browser connection remaining alive unless
the application deliberately chooses that lifetime. Resume must state whether
the cursor means "last received" or "next to read", whether replay can repeat
an event, and what happens after retention expires.

Trigger Sessions and Vercel streams are useful transport references. Duron
already exposes offsets and independent readers. A thin SSE or WebSocket
adapter can translate those positions and handle reconnection without moving
transport concerns into workflow execution.

### A run sleeps while the host is stopped

Duron's recorded sleep deadline remains useful when the host resumes, but the
library cannot execute while its process is absent. Lambda, Inngest, Resonate,
DBOS recovery tooling, and Temporal all demonstrate different owners for this
activation responsibility.

An optional host can own run registration, recovery discovery, and wakeups.
Any design must handle a crash between recording "waiting" and registering its
wakeup. Prefer recoverable discovery or an atomic/outbox boundary over a
best-effort notification. Do not advertise scale-to-zero activation until an
actual host implements it. The existing API need not acquire a mandatory
scheduler to make this ownership clear.

## A sequence that preserves the thesis

This is a proposed order for future work, not a requested implementation or a
commitment to add every feature.

### First, make existing semantics easier to verify and operate

1. Provide a read-only history inspector showing operation identity, attempts,
   retry deadlines, owner changes, pending requests, channel state, and durable
   acknowledgements. Report the first mismatch with expected and actual
   identities. Build from existing entries; avoid another authoritative state
   database.
2. Provide an offline replay compatibility check using real saved histories,
   with effect dispatch disabled. Run it against old and proposed application
   builds. Existing name/version/codec checks remain necessary but do not
   replace this operational check.
3. Turn the backend contract into a reusable conformance suite. Existing tests
   cover fencing, atomic batches, opaque records, cursor reads, and SQLite
   reopen. Add process-crash and ambiguous-append coverage, then make the suite
   available to backend authors before promising that a new backend is equivalent.
4. Publish one end-to-end recovery example with token output, a user command,
   process termination, restart, client reconnection, and an external action
   protected by an idempotency key. Show durable state at each boundary.

The best sources here are Temporal, Durable Task, Golem, and the queue systems'
explicit delivery documentation. These changes explain and exercise the current
runtime rather than widening its purpose.

### Next, define history and deployment lifecycles

Long-lived interaction is the part of Duron's thesis most likely to outgrow
append-only history without explicit policy. Study Continue-As-New and stable
session identity before implementing arbitrary snapshots.

A rollover design must specify:

- A stable application conversation identity versus each `run_id` and effect
  invocation identity.
- A clean boundary with no unresolved operations, or an explicit transfer
  protocol for pending requests, signals, producers, and external workers.
- Versioned serialized application state sufficient to start the next run.
- Continuity or deliberate expiration of output cursors and semantic receipts.
- The behavior of late messages and duplicate reports addressed to an old run.
- Retention, archival, and garbage collection separately from replay rollover.

Finishing one run and creating another with explicit state and lineage fits the
thesis. Deleting a prefix of a live positional history does not, because that
can invalidate operation identities, acknowledgements, and duplicate receipts.
An implementation needs to prove crash safety at the handoff, not just show a
smaller log afterward.

Keep durable-format compatibility separate from Python API evolution. A
versioned application deployment can retain the executable that understands a
matching history, or deliberately start a new run. It must not silently
reinterpret incompatible histories. Avoid compatibility wrappers that preserve
obsolete Python APIs; this repository explicitly prefers clean API breaks.

### Then add optional host and integration tools where use requires them

| Candidate | Evidence to borrow | Smallest appropriate ownership |
| --- | --- | --- |
| Recovery supervisor and wakeups | DBOS, Resonate, Lambda, Inngest | Optional host/application service, with recoverable discovery. |
| Queue admission, tenant limits, and rate control | Hatchet, Inngest, Oban, BullMQ | Dispatch adapter or host policy; durable decisions only where semantics require them. |
| Remote effect liveness and deadlines | Temporal, Cadence, Step Functions | Worker integration with explicit queue, execution, and heartbeat meanings. |
| Retry jitter | Step Functions | Existing retry policy and persisted absolute deadline, if real workloads need it. |
| SSE/WebSocket reconnection | Trigger Sessions, Vercel, Temporal Streams | Transport adapter translating existing durable offsets and closure. |
| Atomic mutation plus enqueue/progress | DBOS, Oban, Absurd | Database-specific adapter or outbox integration with a demonstrable transaction boundary. |
| Compensation examples | Conductor, Golem, Temporal patterns | A recipe using existing effects before a new core abstraction. |
| Agent framework integration | Pydantic AI, LangGraph | A small adapter preserving Duron's effect and lifecycle contract. |

Do not expand all of these simultaneously. The first concrete host use case
should determine which tool is necessary. Embedded use should continue to work
without deploying any of them.

## How to test a proposed improvement

Compare observable outcomes, not just API names or a happy-path demo. Reuse the
same scenarios across Duron and any selected competitor:

| Failure injection | Required observation |
| --- | --- |
| Kill after external success but before result persistence. | The workflow recovers; duplicate action is prevented by the recipient's key or exposed as an ambiguous outcome. |
| Kill after append commit but before its receipt reaches the caller. | Recovery recognizes committed work without conflicting reposts or overwritten history. |
| Emit partial output, then fail or lose the producer. | Attempt identity and retained output match the documented policy; new text is never silently adopted as an old prefix. |
| Apply a command externally, then kill before acknowledgement. | The same message redelivers; business idempotency prevents a second action. |
| Replace the run owner while an old worker reports completion. | Stale authority is rejected; authenticated rebind follows the declared lifecycle. |
| Race stop, stream EOF, producer failure, and final completion. | Channel closure and final outcome remain consistent; EOF never hides a producer failure. |
| Disconnect and reconnect a reader around publication and closure. | The cursor contract explains repeats, gaps, and terminal status. |
| Stop the host past a timer deadline. | Reopening preserves the deadline; any claimed automatic wakeup is provided by a real supervisor. |
| Deploy changed code against an existing history. | Replay either accepts a compatible execution or rejects a precise mismatch before new external dispatch. |
| Expire or remove retained output and receipts. | Old cursors and idempotency keys follow an explicit expiration contract. |

Duron's existing tests already exercise many of these boundaries. This list is
a comparative evaluation protocol, not a claim that the current suite lacks
every scenario or that competitors fail them.

If performance becomes a decision factor, compare the same work with the same
commit acknowledgement boundary and durability settings. Separate replay time,
new-effect latency, activation delay, stream publication latency, and retained
bytes. A queue dispatch number and an fsync-backed stream receipt number do not
measure the same operation. This survey supplies no performance ranking.

## What to keep out of the core

- A mandatory orchestration server, sidecar, message broker, or database service.
- A DAG or asset graph that replaces ordinary Python control flow.
- An LLM agent, connector, or internal-tool platform bundled into the runtime.
- A claim of exactly-once arbitrary external effects.
- Silent history migration, guessed replay compatibility, or regenerated-prefix
  adoption.
- Pickled Python coroutine stacks or process snapshots as a requirement for
  portability.
- Cluster operations added to the minimal backend protocol before a concrete
  host design needs them.

The comparison supports Duron's thesis, but it narrows the positioning. Embedded
execution and durable streaming each have serious competitors. The credible
claim is the particular Python runtime and its explicit contracts: typed
interaction, scoped ownership, acknowledged commands, durable receipts,
positional replay, and storage choice. History lifecycle and operational
clarity are the most useful next investments around that core.
