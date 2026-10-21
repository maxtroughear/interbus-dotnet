# Message pump core refactor

Status: implementation plan. These contracts are proposed, not implemented.

Follow-on: [message pump circuit breaker](message-pump-circuit-breaker.md), for [issue 210](https://github.com/maxtroughear/interbus-dotnet/issues/210).

## Outcome

Move polling loops, acquisition capacity, retry scheduling and worker supervision into the core. Transport authors implement one acquisition iteration or SDK intake control. Preserve SQS buffering and throughput, PostgreSQL transaction ownership, ordering, delivery settlement and graceful shutdown.

Use three explicit source interfaces, without delegate-based source factories:

| Interface | Choose when | Adapters |
| --- | --- | --- |
| `IBufferedPollingSource` | Deliveries can outlive the receive method | SQS, PostgreSQL lease |
| `IScopedPollingSource` | An iteration must retain resources through processing | PostgreSQL transactional, in-memory initially |
| `ICallbackSource` | An SDK invokes delivery callbacks | Azure Service Bus, RabbitMQ |

PostgreSQL lease already implements `IBufferedSource`; this change replaces its transport-owned loop with one claim per iteration and adds configurable prefetch. Preserve its default prefetch of zero.

## Proposed source contracts

Retain `IConsumerSource` initialization, one-shot startup readiness, fatal background failure and asynchronous disposal. `IConsumerSourceFactory` still creates one source per descriptor. Update its documentation to describe all three execution models.

The following signatures describe the intended contracts; complete XML documentation is part of implementation.

```csharp
public interface IBufferedPollingSource : IConsumerSource
{
    int PrefetchCount { get; }
    int MaxBatchSize { get; }

    Task<PollResult> ReceiveOnceAsync(
        ReceiveBatch batch,
        CancellationToken intakeToken);
}

public delegate Task DeliveryHandler(
    IDelivery delivery,
    CancellationToken processingToken);

public interface IScopedPollingSource : IConsumerSource
{
    Task<PollResult> ProcessOnceAsync(
        DeliveryHandler process,
        CancellationToken intakeToken,
        CancellationToken processingToken);
}

public interface ICallbackSource : IConsumerSource
{
    Task StartAsync(
        DeliveryHandler process,
        CancellationToken processingToken,
        CancellationToken operationToken);

    Task PauseAsync(CancellationToken operationToken);
    Task ResumeAsync(CancellationToken operationToken);
}
```

Sources implement exactly one execution interface. Reject invalid combinations at startup.

## Registration and runtime selection

Keep the transport integration point as one `IConsumerSourceFactory` registration. A transport does not register a polling runner, buffer, coordinator, timer, controller or hosted pump service. The implemented source interface selects the execution path automatically; there is no separate execution-model setting to keep synchronized.

For example, the SQS transport extension retains its existing registration:

```csharp
builder.Services.AddSingleton<IConsumerSourceFactory, SqsQueueMessagePumpFactory>();
```

Its factory still constructs an uninitialized per-consumer adapter, including the existing consumer-specific envelope override:

```csharp
public IConsumerSource Create(ConsumerDescriptor descriptor)
{
    var envelope = provider.GetKeyedService<ISnsSqsMessageEnvelope>(
        descriptor.ConsumerType)
        ?? provider.GetRequiredService<ISnsSqsMessageEnvelope>();

    return ActivatorUtilities.CreateInstance<SqsQueueMessagePump>(
        provider, descriptor, envelope);
}
```

`SqsQueueMessagePump` changes to implement `IBufferedPollingSource` and one receive iteration. Its factory requires no knowledge of the core loop or circuit policy. RabbitMQ and Azure factories similarly keep returning their adapters, which now implement `ICallbackSource`.

PostgreSQL registers its source factory once for either supported mode. Validate the mode separately and remove the duplicate registration branches in `RegisterCommonServices`:

```csharp
builder.Services.AddSingleton<IConsumerSourceFactory, PostgreSqlConsumerSourceFactory>();
```

The factory retains the mode choice; it chooses a transport adapter, not a runner:

```csharp
public IConsumerSource Create(ConsumerDescriptor descriptor) =>
    options.MessagePumpMode switch
    {
        PostgreSqlMessagePumpMode.Lease =>
            ActivatorUtilities.CreateInstance<PostgreSqlLeaseMessagePump>(
                provider, descriptor),
        PostgreSqlMessagePumpMode.Transactional =>
            ActivatorUtilities.CreateInstance<PostgreSqlTransactionalMessagePump>(
                provider, descriptor),
        _ => throw new InvalidOperationException("Unsupported PostgreSQL pump mode")
    };
```

Core wiring remains in `AddInterBus`, after the transport configuration callback has registered its factory:

1. Register shared core services and, when a source factory exists, one `PumpController` and `PumpHost`
2. Preserve schema initialization before topology initialization and topology before pump startup
3. `PumpController` asks the factory for one source per active descriptor lifetime
4. Validate exactly one execution interface and its capacity/concurrency settings, then initialize the source
5. `PumpLifetime` creates the per-consumer runtime and selects buffered polling, scoped polling or callback orchestration from that interface
6. That lifetime owns the source and per-consumer runtime state through final drain and disposal

The singleton factory may use DI to obtain shared clients, options and loggers. Construct adapters with `ActivatorUtilities` as today; do not register an adapter as a singleton shared by consumers or let both DI and the lifetime dispose it. Preserve scoped handler/middleware resolution through the existing delivery processor.

Consumer application setup remains the existing `AddInterBus` plus `AddAmazonSqsTransport`, `AddPostgreSqlTransport`, etc. No additional `AddPollingPump`, `AddBufferedRuntime` or per-consumer pump service registration is required. This refactor does not introduce multi-transport source-factory routing.

## Core-owned polling execution

Implement the polling workers inside `ConsumerRuntime` (or internal helpers owned by it), rather than adding a common base class that each transport must subclass. Keep one drain path and shared loop orchestration for cancellation, idle waits, retry scheduling and fatal-error reporting. Model-specific iteration adapters supply buffered reservations or scoped processing; they do not duplicate the surrounding supervisor loop.

The buffered worker algorithm is:

```text
while intake is active:
    wait for admission and at least one ownership slot
    reserve additional available slots up to the batch limit
    try one source.ReceiveOnceAsync(batch, intakeToken)
    dispose batch, returning unused slots even if the call failed
    report acquisition outcome
    on recoverable acquisition failure:
        await retry delay without reservations
        continue through admission
    on empty result:
        await deferred idle wait without reservations
    repeat without awaiting processing of transferred deliveries
```

Start `PumpCount` of these workers over one source and one shared sink. Run the sink drain concurrently with all workers, rather than draining only after receives stop. On terminal stop/failure, prevent further acquisition, await producer exit, complete the sink once, await accepted work and only then dispose source resources. A failure after partial transfer cannot revoke ownership of those deliveries.

The scoped worker uses the same outer scheduling behavior but calls `ProcessOnceAsync` with its processing delegate instead of reserving a receive batch. Start `ConcurrencyCount` workers. Each worker awaits its accepted delivery and surrounding scope before another iteration. Distinguish acquisition failures eligible for retry from processing/settlement/cleanup failures; the shared loop must not mistake the latter for permission to replay processing.

In the refactor, admission permits normal operation unconditionally, subject to capacity and terminal lifecycle state. The follow-on feature supplies reversible circuit admission at this same point. Polling transports never implement their own circuit wait or outer receive/retry loop.

### Buffered ownership

`ReceiveBatch` is a core-created, iteration-scoped reservation with `Capacity` and synchronous `Transfer(IDelivery)` members. It wraps the existing `DeliverySink` reservation mechanism rather than creating a second buffer.

- Reserve capacity before calling the adapter, never after acquisition
- `Capacity` bounds the receive request; adapters cannot reserve extra slots themselves
- Successful transfer immediately gives ownership to the core, without awaiting handler completion
- The adapter disposes acquired deliveries that were not transferred, including partial construction and transfer failures
- Transferred deliveries remain core-owned even if the iteration later throws
- Core disposal of the batch returns only unused reservations
- Reject over-transfer and use after the iteration ends
- A delivery starts required lease/visibility renewal as soon as acquired, including while buffered

The ownership budget is `ConcurrencyCount + PrefetchCount`, shared by every receive pump. It covers reservations, buffered deliveries and active processing. Capacity is released after settlement and disposal, including failure paths.

Core waits for one slot, then takes additional immediately available slots up to `MaxBatchSize`. It never waits for a full batch. Zero prefetch still allows acquisition into free processing slots. Receives continue while earlier deliveries process if capacity remains.

Keep processing concurrency and ordering-key chains in `ConsumerRuntime`. `PumpCount` controls buffered acquisition loops only; it does not multiply ownership or handler concurrency.

### Scoped ownership

Core creates `ConcurrencyCount` workers and admits each iteration before the adapter opens resources. Retain `PumpCount = 1` for scoped sources to preserve current configuration semantics.

Each iteration acquires at most one delivery and awaits `process` through settlement and delivery cleanup. The adapter then disposes surrounding resources before returning. Core owns delivery disposal after successful callback invocation; the adapter owns surrounding transaction/connection disposal and any delivery not passed to core.

For PostgreSQL, preserve the ambient transaction in the same async execution flow that invokes and awaits processing. Do not buffer transaction-bound deliveries or replay a processing callback because a transaction operation failed.

### Runtime boundaries between polling models

Share the polling supervisor, not every execution step. Buffered iteration completion means acquisition and ownership transfer have finished; scoped iteration completion means processing, settlement and surrounding resource cleanup have finished. A scoped worker slot stays occupied until the iteration returns, even after `DeliveryProcessor` returns.

Define an explicit delivery-acceptance signal or structured failure stage for scoped iterations. An exception before acceptance can be evaluated by the acquisition retry policy. An exception after acceptance, including ambiguous commit or cleanup failure, must not be treated as permission to replay processing. Propagate its structured outcome to supervision and transport health observation according to the existing recovery semantics.

Do not automatically reuse the buffered ordering queue for scoped sources. Waiting for an ordering predecessor after opening a transaction can hold database resources indefinitely. Scoped adapters must acquire work in an order safe for immediate processing, or obtain ordering admission before opening the scope where possible. Document and test the ordering guarantees for each scoped adapter, including in-memory redelivery.

Both polling interfaces allow concurrent iteration calls on the same source instance. Keep connections, transactions, reservations and acquisition metadata local to each iteration; synchronize shared listener and notification state. For scoped circuit probes, do not publish successful recovery until required enclosing scope completion is known.

### Empty polls and idle waits

Introduce a `PollResult` describing whether acquisition found work and an optional deferred idle wait. The wait can be represented by `Func<CancellationToken, ValueTask>`; it must not start until core invokes it.

The result is about acquisition, not acknowledgement. A claimed message that is released or dead-lettered is not an empty poll.

For an empty poll, core releases reservations before invoking the wait. The adapter has already closed any acquisition transaction. After the wait, core reacquires admission and capacity. This is also the extension point for releasing half-open permits before idle waits.

PostgreSQL captures its existing work version before querying and supplies a deferred notification/fallback wait using that version. Preserve the version check to avoid lost wakeups. SQS empty long polls can return an empty result with no additional wait. In-memory waits for readability without removing a message, then the next admitted iteration attempts removal.

## Retry and health behavior

Core schedules polling retries; adapters retain transport-specific exception classification and existing retry options. Define an optional polling retry-policy contract returning retry/terminal and delay for an exception and consecutive attempt number. Default to terminal when no policy is supplied.

- Release unused reservations before retry delays
- Reenter admission and reserve fresh capacity for every explicit retry
- Reset acquisition retry counts after a successful acquisition operation, including an empty response
- Preserve SQS's current five-second receive retry behavior and PostgreSQL's connection-error filtering and configured limits
- Do not wrap processing, settlement or an entire transaction lifetime in an automatic replay loop
- SDK-internal retries remain adapter behavior; document that pause cannot instantly interrupt every SDK operation
- Initialization retries and PostgreSQL listener reconnection remain separate from receive-loop retries
- Fatal background failures continue to fault the lifetime promptly, even while workers are idle or paused

Add an internal acquisition observation hook at iteration boundaries. It initially has no circuit behavior. Acquisition errors, delivery outcomes and fatal source failures remain distinct signals.

## Callback intake and lifetime

`StartAsync` installs callbacks and starts intake once. It returns when intake startup succeeds; the source stays supervised through its fatal failure signal. Preserve one-shot readiness semantics.

`PauseAsync` suspends new intake as promptly as the adapter permits. It may await callback drain, but must not cancel accepted processing, stop required renewal, or dispose resources needed for settlement. Already scheduled callbacks are accepted work and must be accounted for. `ResumeAsync` reinstates intake without duplicating callback registration.

Core serializes start/pause/resume and terminal shutdown operations outside delivery callbacks. Terminal shutdown marks the lifetime as stopping before waiting for any control operation, prevents later resume, stops intake and drains accepted work. Operation tokens bound control work; they are not processing tokens. A canceled or failed control operation must have a defined state and cannot silently leave intake running.

Source disposal remains terminal and occurs only after accepted callbacks stop using its resources. A processing deadline may cancel handlers without allowing premature resource disposal. Do not implement pause by calling `PumpLifetime.StopAsync` or recreating the lifetime.

Azure must explicitly distinguish a pause-related SDK callback cancellation from a real processing cancellation. Its current suppression checks only terminal intake cancellation. RabbitMQ must retain channels for outstanding acknowledgements and coordinate consumer tags with connection recovery.

Validate those mechanisms against the actual SDKs before finalizing the callback contract. If stop waits for callbacks, core must remain able to observe shutdown deadlines and faults while the control operation is pending.

## Adapter migration

| Adapter | Work |
| --- | --- |
| SQS | Replace receive loop with one request using `batch.Capacity`; keep maximum batch size 10, current prefetch settings, whole-batch renewal and delivery settlement |
| PostgreSQL lease | Replace loop with one claim, `MaxBatchSize = 1`; add nonnegative lease-mode `PrefetchCount`, default zero; keep renewal and failure redelivery delay |
| PostgreSQL transactional | Extract one transaction-scoped claim/process iteration; keep notifications, ambient context, commit/rollback and connection retry classification |
| In-memory | Use scoped iterations initially; separate readability waits from removal; preserve delivery counts, retry timing and ordering, with each redelivery attempt returning through core admission |
| Azure Service Bus | Implement reversible SDK intake control for normal and session processors, preserving active callback ownership |
| RabbitMQ | Implement reversible subscription control, accounting for queued callbacks, prefetch, recovery and channel generations |

The in-memory adapter currently performs redelivery inside an accepted envelope's loop. Extract its retry state deliberately; the runtime must not interpret successive attempts as unrelated messages or allow later ordered work to overtake a pending redelivery.

## Implementation sequence

1. Add the three contracts, reservation batch, poll result and retry policy. Document ownership and cancellation boundaries. Add fake adapters for core tests and define automatic runtime selection through the existing factory registration.
2. Implement core buffered polling over the existing sink and drain path. Migrate SQS and PostgreSQL lease together, preserving current defaults.
3. Implement core scoped workers. Migrate transactional PostgreSQL and in-memory, including idle waiting and retry state.
4. Implement callback control serialization and migrate Azure/RabbitMQ. Prove actual SDK stop/drain/restart behavior before freezing the public contract.
5. Update `PumpLifetime`, source factories, runtime selection and test doubles. Preserve one factory registration per transport, consolidate PostgreSQL mode registration and verify startup ordering. Keep old interfaces temporarily only as needed for intermediate commits; remove them before completion and avoid maintaining duplicate engines.
6. Update the message-pumps reference and transport authoring guidance. Treat removal of the public old interfaces as a breaking API change and record migration instructions.

Keep this refactor independently usable with admission always enabled. Circuit thresholds, timers and user-facing circuit configuration belong to the follow-on feature.

## Validation and completion

Use controllable fake sources and task barriers for core concurrency tests. Extend existing runtime/lifetime suites rather than replacing their ownership regression coverage.

- No receive call at zero capacity; no full-batch wait; multiple pumps share one budget
- SQS receives again while earlier handlers are blocked when prefetch allows; owned deliveries never exceed the budget
- Partially filled batches, empty responses, partial transfer failures, cancellation and cleanup failures return exactly the correct slots
- Buffered work renews, drains and preserves ordering; handler concurrency remains bounded
- Scoped transactions remain active through settlement, with no connection held during idle waits
- Scoped commit/cleanup failures cannot trigger acquisition replay; worker slots remain occupied through surrounding scope cleanup
- Scoped ordering does not wait in the buffered ordering queue while holding a transaction; concurrent iterations do not share per-delivery state
- Notification races cannot strand work until an unnecessary fallback timeout
- Explicit retries release reservations and reenter admission; fatal failures still reach the host
- Repeated callback pause/resume does not duplicate registrations or cancel accepted work
- Shutdown during acquisition, idle wait, retry delay or callback control honors the existing drain/deadline rules
- PostgreSQL lease prefetch is configurable, bounded and defaults to zero
- In-memory redelivery behavior and FIFO/session behavior remain intact
- Register each transport through its public extension only and verify the correct core execution path is selected without runner registrations
- Both PostgreSQL modes register exactly one source factory; consumer adapters and ownership budgets are distinct per lifetime
- Producer-only setups do not start consumer loops; schema/topology setup precedes acquisition and each source is disposed once

Run core runtime/lifetime and existing SQS, PostgreSQL, Azure and RabbitMQ adapter regression suites. Run available broker/database integration suites and report missing infrastructure explicitly. Compare SQS before/after throughput under identical batch, concurrency and prefetch settings, recording receive-call volume, maximum ownership and handler utilization; investigate any regression before completing the refactor.
