# Message pump implementation plan

## Purpose and scope

Complete the message pump refactor on this branch, using SQS as the first complete implementation, then migrate the other transports. This document is the implementation handoff; implementation has not started.

Baseline reviewed: `ac71654` (`more todos`). Read `AGENTS.md` and inspect the current checkout before making changes, since this plan may be implemented after further branch changes.

The agreed architecture shares core message processing across all transports, while supporting two scheduling models:

| Model | Transport owns | Core owns |
| --- | --- | --- |
| Buffered | Receiving, envelope decoding, broker ownership/renewal and settlement operations | A per-consumer bounded buffer, dispatch workers, processing, outcome handling and lifecycle supervision |
| Transport-managed | Scheduling and delivery/transaction/session lifetime | Processing invoked and awaited within that lifetime, outcome handling and lifecycle supervision |

Use buffered execution for SQS and PostgreSQL lease mode. Use transport-managed execution for PostgreSQL transactional mode, Azure Service Bus and initially RabbitMQ. Use core workers for in-memory delivery, reusing its broker channel where practical.

All transports must use the shared processing operation and core lifecycle management. Transport-managed execution is a supported architecture, not a bypass of core. Do not force SDK callbacks or transaction-owning deliveries through another buffer.

Keep request/reply virtual queue implementations functioning. Migrating their separate receive loops is outside this change unless a shared interface change requires an adaptation. Do not combine unrelated test cleanup or behavior changes with this refactor.

## Phase 1: Define the shared contracts and consumer runtime

Primary files: `src/InterBus/MessagePumps/*`, `src/InterBus/Dispatching/Dispatcher.cs`, consumer descriptor/builders and core service registration.

### Processing contract

Replace the current settlement-only `IMessage` prototype with a delivery contract sufficient for core processing:

- Decoded payload and headers, associated with a consumer descriptor
- Acknowledge, release/abandon and dead-letter operations
- Asynchronous cleanup of ownership renewal and delivery resources
- An optional ordering key for transports requiring serialized processing within a group
- A way to distinguish successful settlement from rejected/lost ownership; never report a rejected settlement as success

Exact names are implementation choices. Keep transport handles, receipt tokens, SDK event arguments and database resources in transport-specific delivery implementations. Preserve raw wire data where dead-letter forwarding needs it. Decide intentionally which types need to be public; document public contracts completely, including ownership and cancellation semantics.

Build one core processing operation, conceptually `ProcessAsync(descriptor, delivery, cancellationToken)`, usable by both scheduling models. It must:

1. Create a fresh DI scope and invoke the existing dispatch pipeline, retaining telemetry, middleware, consumer routing and scope disposal
2. Map `DispatchResult` to the transport's settlement operation
3. Log the actual outcome, with settlement success reported only after settlement succeeds
4. Handle decoding/dispatch failures and settlement failures without losing ownership cleanup
5. Always perform asynchronous cleanup, including when processing or settlement throws

Specify failure behavior before coding: a failed/ambiguous acknowledge or dead-letter operation must not cause core to blindly perform a conflicting second settlement. Release only where the transport can safely support it; otherwise stop renewal and allow broker recovery. Retain at-least-once delivery semantics, without promising exactly-once settlement.

Envelope decoding remains transport-specific. Ensure a decoding failure still has an owned delivery that can be released/cleaned up; do not abandon ownership merely because decoding failed before core invocation.

### Scheduling contracts

Replace `SelfContained` and the placeholder core task with explicit buffered and transport-managed contracts. Prefer separate interfaces/adapters over a boolean that allows invalid combinations.

- Buffered sources hand deliveries into an injected core sink. Define ownership transfer precisely: source owns a delivery until handoff succeeds; core owns it afterward
- Managed sources call and await core processing within their callback/transaction boundary
- Both expose lifecycle operations allowing core to initialize, run, stop and asynchronously dispose them
- The lifecycle contract must accommodate callback-based SDKs that start processors rather than run a polling loop

For buffered execution, create one runtime per consumer descriptor, not one runtime per receive pump:

- `PumpCount` receive pumps feed one buffer
- `ConcurrencyCount` bounds total active processing across those pumps
- Prefetch capacity belongs to the shared runtime; it must not multiply with `PumpCount`
- Multiple producers require channel settings that support multiple writers
- Ordering constraints serialize a key through settlement, while allowing independent keys to progress

Define and test capacity accounting across active processing, buffered deliveries and outstanding receives. Reserve capacity before issuing receive/claim operations so multiple pumps cannot each consume the same apparent free slots. Return reservations on empty receives, errors, cancellation and cleanup. SQS batches must fit reserved capacity, up to its existing batch limit of ten.

Preserve the meaning of SQS `PrefetchCount = 0`: no additional prefetch beyond available processing capacity. The installed .NET 10 runtime accepts zero-capacity bounded channels; do not introduce a false validation rule that requires positive prefetch. Channel capacity alone does not account for deliveries already received but waiting to be enqueued.

Add a validated builder API for `PumpCount`. For this iteration, treat it as a buffered receive-source setting. Managed transports use one managed adapter per consumer and configure total concurrency internally. Reject explicit `PumpCount > 1` for those adapters with a clear configuration error rather than silently ignoring it or multiplying concurrency. Document this limitation.

Acceptance:

- A fake buffered transport with several sources never exceeds the configured consumer concurrency or ownership budget
- A fake managed transport awaits the same processing and settlement operation without using a core buffer
- Scope isolation, all dispatch outcomes, ordering keys, cancellation, lost ownership and settlement failures have deterministic unit coverage

## Phase 2: Complete lifecycle, supervision and registration

Finish `PumpHost`, `PumpController`, `PumpLifetime`, `PumpState` and cleanup before switching transport registrations.

- Choose a single hosting implementation. Remove hidden `BackgroundService.StartAsync`/`StopAsync` methods and duplicated startup paths
- Preserve schema/topology-before-consumption ordering, and producer-only startup
- Honor the host startup cancellation token during initialization, including rollback after partial startup
- Track initialization, receive/managed-source tasks, dispatch workers, renewal resources and asynchronous disposal
- Observe faults promptly; do not leave a completed/faulted pump marked `Running`
- Add a fault terminal state or equivalent explicit fault reporting. Retain failures long enough for diagnostics
- Retry recoverable transport connection failures inside the adapter's existing policy. Surface unhandled failures through the hosted service so the host's configured failure policy can apply
- Serialize start/stop transitions. A stop during initialization must not allow work to start afterward; repeated/concurrent stop calls must await the same shutdown
- Return collection snapshots from the controller, not `AsReadOnly()` wrappers over a mutable list
- Dispose stopped lifetimes before removing them, and dispose adapter-created resources as well as cancellation sources
- Stop all consumers even if one fails during shutdown; aggregate/report failures afterward

Separate intake cancellation from dispatch/renewal cancellation. Default graceful shutdown should:

1. Stop new receives/claims or managed intake
2. Await producers and clean up deliveries that were received but not transferred
3. Complete buffered input and drain accepted work while keeping ownership renewal active
4. On the host shutdown deadline, cancel processing and perform bounded best-effort transport cleanup
5. Await remaining work within the shutdown budget and dispose resources only when no task still uses them

Treat expected cancellation as normal termination, guarantee terminal state updates in cleanup paths, and report incomplete shutdown if a handler ignores cancellation. For SDK-managed transports, use the SDK stop mechanism to preserve callback and renewal lifetimes through shutdown.

Register the host/controller once in core after transport configuration has supplied the needed adapter factory. Keep each transport's old registration until that transport is migrated; avoid requiring an unimplemented factory for an old transport, and never run old and new pumps for the same consumer. A temporary explicit opt-in to the new runtime is acceptable during the migration; remove it when all transports are converted.

Acceptance:

- Unit tests cover initialization failure, partial startup, receive/worker faults, unexpected completion, stop during initialization, repeated stop, deadline cancellation and disposal
- Real host startup/stop is exercised, not just manually starting hosted services with a `ServiceCollection`
- No unobserved tasks, stuck `Starting`/`Stopping` states or disposed resources accessed by active tasks

## Phase 3: Complete and activate SQS

Primary files: `src/InterBus.AmazonSqs/MessagePumps/*`, `SnsSqsTopologyBuilder.cs`, transport registration/options and `tests/InterBus.AmazonSqs.Tests/*`.

- Turn `SqsQueueMessagePump` into a buffered receive adapter. Remove its private channel, dispatch workers, unused semaphore and result handling
- Decode with the existing per-consumer keyed/default envelope selection, retaining raw and SNS-serialized formats and direct sends
- Use core capacity reservations when calculating receive batch sizes
- Start renewal for every received message before awaiting any handoff. The existing lazy `Select` leaves later batch members unrenewed while a write blocks
- Implement cleanup for received-but-unenqueued deliveries, buffered deliveries and active deliveries
- Keep renewal active through processing and settlement, and stop/await/dispose it on every exit path. `Dispose()` alone must not leave a renewal loop running
- Preserve default release timing: existing active SQS code lets visibility expire. Do not silently switch all releases to visibility zero; make immediate release an explicit policy if needed
- Handle invalid receipts, failed renewal, delete failures and dead-letter send/delete failures without indefinite renewal or a false success log
- Preserve FIFO ordering within a message group when concurrency or receive-pump count exceeds one. Request/extract the necessary group metadata and supply an ordering key to core
- Preserve dead-letter diagnostics, raw data and applicable FIFO send metadata
- Validate negative counts and invalid polling/visibility/renewal configuration, keeping the existing supported zero-prefetch setting
- Preserve create/verify and resolve-only topology behavior and connection retry ordering
- Replace the old hosted pump registration only after the new route passes the acceptance suite

Tests must prove that the new adapter is the one registered and executed. Cover default settings, positive prefetch, several receive pumps, slow consumers, renewal while blocked, malformed envelopes, cancellation during batch handoff, each settlement failure, both envelope formats, FIFO group ordering and independent-group concurrency. Existing publish/consume, dead-letter and request/reply tests must pass on the new registration.

## Phase 4: Migrate PostgreSQL lease mode

Primary files: `PostgreSqlMessagePumpBase.cs`, `PostgreSqlLeaseMessagePump.cs`, schema helpers, registration and PostgreSQL tests.

- Extract a lease delivery containing payload/headers, message/payload IDs and its unique lease token
- Move dispatch, outcome selection and generic logging into core; keep claim/renew/ack/release/dead-letter SQL in the transport
- Feed the buffered runtime with reserved capacity. Do not claim messages that cannot be owned and renewed while waiting
- Preserve lease-token/expiry checks, delayed failure redelivery and stale-settlement rejection
- Keep `LISTEN`/`NOTIFY`, fallback polling, connection retry and orphan cleanup in transport infrastructure. Do not multiply queue-level maintenance work unnecessarily with `PumpCount`
- Replace the current ad hoc worker list/semaphore with the shared runtime
- Before any listener restart, cancel and drain its remaining work in `finally`. Today an exceptional exit can skip `DrainWorkersAsync`, dispose the semaphore and restart alongside live workers
- Keep renewal active through settlement and clean it up on non-cancellation failures too

Acceptance: test concurrency limits and multiple sources, renewal during queueing and long handlers, expiry/reclaim, stale ownership rejection, delayed release, failed dispatch/settlement, reconnect while work is active and shutdown. Run PostgreSQL 17/18 coverage. Restrict lease-specific stall tests to lease fixtures; the current `LeasePumpStallTests` inherits the all-mode fixture source.

## Phase 5: Migrate PostgreSQL transactional mode

Use transport-managed execution. A transport worker opens a connection/transaction, claims one row, establishes `PostgreSqlTransactionContext`, awaits core processing and then clears/disposes resources in `finally`.

- Keep row ownership and the ambient transaction alive through consumer execution and settlement
- The delivery's acknowledge/dead-letter operations perform their SQL and commit within the owning transaction; release rolls back
- Guarantee rollback/disposal after dispatch or settlement failure, including failed initialization of a delivery
- Keep transaction context establishment and core invocation in the same async execution flow. Do not assume an `AsyncLocal` assignment inside a separately awaited setup helper flows back into its caller
- Configure `ConcurrencyCount` transaction-owning workers in one managed adapter, without an additional delivery buffer
- Retain transport notification/retry infrastructure where useful, with the lifecycle guarantees from Phase 2
- Preserve the existing shared-transaction consumer API

Restore the removed `Transactional_Consumer_CanAccessSharedTransaction` and `Transactional_EachDispatch_ReceivesNonNullTransaction` tests. Add concurrent transaction isolation, rollback/commit, cancellation, settlement failure and connection-disposal coverage. Verify ambient state cannot leak between deliveries or consumers.

## Phase 6: Migrate Azure Service Bus

Use transport-managed execution with one normal/session processor adapter per consumer.

- The processor callback wraps the SDK delivery and awaits core processing through settlement and cleanup before returning
- Keep concurrency, sessions and auto-lock renewal in the SDK. Map `ConcurrencyCount` to the current normal/session options
- Preserve one concurrent call per session and independent-session concurrency
- Delivery operations delegate to complete, abandon and dead-letter APIs using the correct callback context
- Core supervises processor startup, errors and stop/disposal; do not turn every recoverable SDK error callback into an unconditional host failure
- Do not add an external buffer or return from a callback while core processing is still running

Acceptance: normal/session delivery, result mapping, long processing/lock renewal, cancellation, lock loss, SDK error reporting, partial startup cleanup and shutdown with callbacks in flight. Assert ordering within a session and concurrency across sessions.

## Phase 7: Migrate RabbitMQ and in-memory

### RabbitMQ

Use transport-managed execution initially. This preserves SDK dispatch concurrency and avoids introducing a second prefetch/buffering layer.

- Wrap each delivery and await core processing inside `ReceivedAsync`
- Keep channel QoS, consumer dispatch concurrency, delivery tags, connection retry/recovery and channel lifecycle in the transport
- Preserve individual acknowledgement semantics and dead-letter publishing behavior
- Keep delivery buffers within callback lifetime; copy them before returning if any later work retains them
- Stop intake and await callbacks before closing channels/connections

Acceptance: bounded concurrency, ack/nack/dead-letter outcomes, connection recovery, shutdown with active callbacks and no settlement against disposed channels. A later buffered implementation is optional, outside this plan.

### In-memory

- Reuse the core worker/processing implementation over broker envelopes; reuse the broker channel as the runtime input where practical rather than adding a redundant buffer
- Honor `ConcurrencyCount`, retaining sequential behavior at one
- Preserve delivery-count limits, release delay, dead-letter discard behavior, headers, retry middleware, outbox and request/reply behavior
- Correct the channel's current `SingleReader = true` assumption if core introduces multiple readers
- Keep `PumpCount` as a buffered intake setting only if multiple intake sources are actually supported; otherwise reject values above one explicitly
- Define shutdown behavior for queued envelopes without overstating in-memory durability

Acceptance: concurrency bounds, sequential delivery at one, retries/delivery limits, middleware and scope isolation, scheduling, outbox, request/reply and shutdown.

## Phase 8: Remove transitional code and verify parity

- Remove old pump implementations/registrations after their replacements pass parity tests
- Remove `SelfContained`, placeholder tasks, unused dispatch-task bookkeeping and transport-local copies of core processing
- Remove or fold the cleaner if deterministic lifecycle ownership makes the timer unnecessary
- Keep transport connection retry policies and virtual reply hosts intact
- Update public XML documentation and user documentation for execution models, concurrency, prefetch, supported pump counts, ordering and shutdown
- Keep SDK-managed and transactional execution first-class in the final interfaces

Run targeted tests as each phase lands, then the relevant core and transport suites. Exercise real host registration and startup as well as fake-adapter unit tests. Container/cloud tests require their existing fixtures and credentials; report unavailable suites explicitly. Keep high-volume throughput tests separate from small deterministic concurrency assertions.

Final acceptance:

- Every consumer pump uses core processing and lifecycle supervision
- Buffered consumers have one shared processing limit and buffer regardless of receive-pump count
- Managed consumers preserve SDK/session/transaction ownership through settlement
- Every owned delivery has a defined cleanup path, including failed handoff, processing failure, settlement failure and shutdown
- No false outcome logs, unobserved worker faults or indefinite renewal of discarded deliveries
- Existing transport behavior remains compatible except for documented, deliberate configuration validation

## Review evidence and cautions

- SQS/core and PostgreSQL builds passed during review. Core produced warnings for the hidden `PumpHost` start/stop methods
- An isolated fake-pump reproduction showed a faulted loop still marked `Running`, and normal cancellation throwing from `StopAsync` with state left `Stopping`
- An isolated fake-SQS reproduction showed renewal continuing after failed acknowledgement
- Transport integration suites were not run during review
- The existing SQS integration registration still exercises the old pump, so passing it alone does not validate this refactor
- The PostgreSQL load test does not establish a strict concurrency bound; add coordinated blocking-consumer tests
- Some test consolidation removed transaction-context coverage; restore it rather than treating the smaller suite as established parity

Implement phases in order. Keep each transport switch reviewable and runnable, with the preceding runtime contracts settled before broad transport migration.
