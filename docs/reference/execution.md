# `duron.execution` module

`duron.execution` contains the `EffectExecutor` protocol, retries, and typed
channels. Pass an executor implementation to `@duron.effect(executor=...)` for
remote work. Pass `codec=` and `codec_id=` directly to `@duron.effect` to select
an effect-specific codec.

The principal types are `EffectExecutor`, `RetryPolicy`, `StreamSender`,
`CommandSender`, `CommandReceiver`, and `Command`. Remote dispatch lives in
[`duron.execution.remote`](remote.md).

## Retries and recovery

`RetryPolicy()` defaults to one logical attempt, no retryable error types, and
unlimited crash recovery. A reported exception is retried only when its exact
type appears in `retry_on` and the logical attempt budget remains. Increasing
`max_attempts` alone is insufficient. Retry types are automatically declared
for error reconstruction.

After failed attempt `n`, the delay is
`min(max_delay, initial_delay * multiplier ** (n - 1))` seconds. Defaults are
`initial_delay=0`, `multiplier=2`, and `max_delay=60`. The deadline is durable.

`max_recoveries` independently limits reinvocations after an ambiguous outcome:
`None` is unlimited, and `0` allows none. Exhaustion raises
`EffectOutcomeUnknownError`. `EffectContext.idempotency_key` stays stable across
both policy retries and recovery; an external service must honor it to
deduplicate work. See [errors](errors.md) for reconstruction and cancellation.

## Scoped effect channels

`WorkflowContext.open_stream(T)` yields a `DurableStream[T]`. Pass its `sender`
as a named `StreamSender[T]` effect argument. Close the sender explicitly, or
use `async with sender` in the effect, to signal EOF. Readers and final effect
results are independent, so also await the effect task. `stream.reader()`
starts an independent reader from the beginning; direct iteration shares the
stream's default reader. `prefetch` bounds each reader's decoded buffer.

`WorkflowContext.open_commands(T)` yields `DurableCommands[T]`. Pass its
`receiver` as a `CommandReceiver[T]` effect argument. Send workflow commands
with `await commands.sender.send(value)`. The effect calls
`command = await receiver.receive()` and `await command.ack()` after handling
`command.value`. There can be only one outstanding delivery per receiver
owner. Unacknowledged work can be delivered again after failure, so its external
actions must be idempotent too. Commands do not interrupt Python code; the
effect must receive and handle them.

Handles must stay inside their opening workflow scope and cannot be encoded as
application values. Scope cleanup owns channel closure and producer cleanup.
It cannot roll back an external action or forcibly terminate a detached worker.
Explicit send keys deduplicate the same semantic post across recovery;
conflicting reuse raises `PostConflictError`. Omitted effect send keys identify
new invocation-local work.

## ::: duron.execution
