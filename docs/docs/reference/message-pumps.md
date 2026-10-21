---
title: Message pumps
---

Each consumer has one supervised runtime and a fresh DI scope for every dispatch. All transports use the same middleware pipeline and outcome handling. Successful settlement is logged only after the transport accepts it. A rejected ownership token is logged as a rejected settlement.

## Concurrency and intake

`consumer.WithConcurrency(count)` sets the total processing concurrency for a consumer. The default is one. Counts must be positive.

`consumer.WithPumpCount(count)` controls buffered receive sources, with a default of one. It shares the consumer's concurrency and ownership budget; it does not create additional dispatch capacity.

| Transport | Execution | Supported pump count |
| --- | --- | --- |
| Amazon SQS | Buffered polling | One or more |
| PostgreSQL lease | Buffered polling | One or more |
| PostgreSQL transactional | Scoped polling | One |
| Azure Service Bus | SDK callbacks | One |
| RabbitMQ | SDK callbacks | One |
| In-memory | Scoped polling | One |

Scoped and callback adapters reject pump counts above one. PostgreSQL transactional workers retain their connection, transaction and ambient transaction context until processing and settlement finish. Azure processors retain SDK lock renewal and session scheduling. RabbitMQ retains channel QoS and SDK dispatch concurrency. Each consumer runtime owns a dedicated consume connection; publishing retains a shared connection.

SQS `PrefetchCount` is extra capacity beyond `ConcurrencyCount`. Zero is supported and means no extra prefetch. One shared ownership budget covers outstanding receives, queued deliveries and active processing, across all receive pumps. Each batch fits reserved capacity and contains at most ten messages. PostgreSQL lease mode exposes the same nonnegative `PrefetchCount` ownership setting, defaulting to zero. Its claim batch size is one.

SQS polling accepts 0–20 seconds. Renewing SQS consumers require a visibility timeout of at least one second and at most twelve hours, and a positive renewal interval shorter than visibility. Negative prefetch is rejected. Set an explicit subsecond renewal interval when using a one-second visibility timeout.

## Ordering

SQS FIFO message groups are serialized in handoff order through settlement and cleanup; different groups can run concurrently. Azure sessions allow one callback per session, with `ConcurrencyCount` concurrent sessions. Other buffered deliveries have no ordering key unless their adapter supplies one. Concurrency one keeps in-memory processing sequential, including retry attempts. Raising concurrency permits overlap and changes completion order.

## Settlement and failures

Success acknowledges the delivery, release requests redelivery, and dead-letter forwards or discards according to the transport. SQS release stops renewal and lets visibility expire; it does not set visibility to zero. PostgreSQL lease release preserves the configured failure redelivery delay and validates the unique lease token. In-memory retains the existing delivery limit and release delay, and discards dead-lettered envelopes.

A failed SQS renewal marks ownership uncertain and rejects subsequent settlement; cleanup stops renewal so the broker can recover the message. Transient SQS topology connection failures use `ConnectionRetry` before consumption starts.

A failed or ambiguous acknowledge or dead-letter operation is not followed by a conflicting second settlement. Cleanup stops renewal and allows broker recovery. PostgreSQL transactional resources are disposed and uncommitted work rolls back. Delivery remains at least once; settlement is not exactly once.

## Startup and shutdown

Schema and topology initialization precede consumption. Producer-only hosts remain supported. Initialization honors startup cancellation, and partially initialized resources are disposed. Unexpected source completion and unhandled faults are retained in `PumpController.Pumps`, which returns a snapshot. Background faults are reported to the host while accepted deliveries drain, so the configured hosted-service failure policy applies.

Graceful shutdown stops intake first, then drains accepted work while retaining delivery ownership and renewal. SDK callbacks and database transactions remain alive through settlement. The host shutdown deadline cancels processing, including when a controller already started graceful shutdown. Concurrent stop calls share cleanup work and honor each caller's cancellation deadline. If a handler ignores cancellation, shutdown reports incomplete work and keeps resources alive until their users return. Adapters must honor cancellation for transport operations.

In-memory shutdown finishes the active delivery attempt. A released envelope returns through core admission before another attempt, so shutdown can stop pending retries. Envelopes still in the broker channel are not drained during shutdown and have no durability guarantee. Request/reply virtual receive loops retain their transport-specific lifecycle.

## Transport adapter contracts

Register one `IConsumerSourceFactory` through the transport extension. The factory constructs an uninitialized adapter per consumer; the lifetime initializes and disposes it once. Do not register adapters as shared singletons, or register transport-specific runners. `AddInterBus` automatically selects execution from exactly one interface:

- `IBufferedPollingSource`: expose `PrefetchCount` and positive `MaxBatchSize`, then perform one receive in `ReceiveOnceAsync`. Use `batch.Capacity` as the request bound. `batch.Transfer(delivery)` synchronously transfers ownership to core processing. Dispose untransferred deliveries on every failure path. Start renewal as soon as deliveries are acquired
- `IScopedPollingSource`: perform one claim and invoke and await `process` at most once in `ProcessOnceAsync`, then close the surrounding scope. Core disposes the delivery; the adapter disposes connections and transactions. Keep ambient transaction state in the same async flow as processing
- `ICallbackSource`: install callbacks once in `StartAsync`, stop new intake in `PauseAsync`, and restore intake in `ResumeAsync` without reinstalling callbacks. Pause must account for scheduled callbacks and retain settlement resources. Processing tokens belong to accepted work; operation tokens only bound SDK controls

Both polling interfaces permit concurrent calls on one adapter. Keep per-delivery resources local. Return `PollResult(true)` whenever acquisition found a delivery, regardless of its settlement outcome. For empty polls, return `PollResult(false, deferredWait)` after releasing acquisition resources. Core releases unused reservations before invoking that wait. PostgreSQL waits retain the pre-query notification version; in-memory waits observe readability without removing messages.

The core starts `PumpCount` buffered workers or `ConcurrencyCount` scoped workers. Scoped sources bypass buffered ordering chains: PostgreSQL obtains database ownership before immediate processing; in-memory preserves sequential retries at concurrency one. Increasing in-memory concurrency permits overlap, as before.

Implement optional `IPollingRetryPolicy.GetRetryDelay(error, attempt)` to classify acquisition errors. A null delay is terminal. Core owns scheduling, releases reservations before delays, and resets attempts after successful acquisitions including empty responses. After a scoped adapter invokes `process`, any iteration failure is terminal to that runtime and cannot trigger acquisition replay. Initialization retries and listener reconnection remain separate. SDK retries can delay cancellation of an intake operation.

`Ready` is a one-shot startup signal; `Failure` reports fatal background failures even during idle waits or callback control. `PumpLifetime.PauseAsync` and `ResumeAsync` serialize callback controls. Terminal shutdown prevents subsequent resumes, stops intake, drains accepted work and then disposes the original source. A failed control faults the lifetime and triggers terminal intake shutdown.

## Breaking API migration

`IBufferedSource` and `IManagedSource` have been removed. Replace a buffered source's `ReceiveAsync(DeliverySink, ...)` loop with `IBufferedPollingSource.ReceiveOnceAsync(ReceiveBatch, ...)`; remove transport reservation and retry loops. Replace transaction-owning managed workers with `IScopedPollingSource`, and SDK managed workers with `ICallbackSource`. Keep the existing factory registration and application transport configuration. No circuit configuration is introduced by this change.
