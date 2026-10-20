---
title: Message pumps
---

Each consumer has one supervised runtime and a fresh DI scope for every dispatch. All transports use the same middleware pipeline and outcome handling. Successful settlement is logged only after the transport accepts it. A rejected ownership token is logged as a rejected settlement.

## Concurrency and intake

`consumer.WithConcurrency(count)` sets the total processing concurrency for a consumer. The default is one. Counts must be positive.

`consumer.WithPumpCount(count)` controls buffered receive sources, with a default of one. It shares the consumer's concurrency and ownership budget; it does not create additional dispatch capacity.

| Transport | Execution | Supported pump count |
| --- | --- | --- |
| Amazon SQS | Buffered | One or more |
| PostgreSQL lease | Buffered | One or more |
| PostgreSQL transactional | Transport managed | One |
| Azure Service Bus | SDK callbacks | One |
| RabbitMQ | SDK callbacks | One |
| In-memory | Core workers reading the broker channel | One |

Managed adapters reject pump counts above one. PostgreSQL transactional workers retain their connection, transaction and ambient transaction context until processing and settlement finish. Azure processors retain SDK lock renewal and session scheduling. RabbitMQ retains channel QoS and SDK dispatch concurrency. Each consumer runtime owns a dedicated consume connection; publishing retains a shared connection.

SQS `PrefetchCount` is extra capacity beyond `ConcurrencyCount`. Zero is supported and means no extra prefetch. One shared ownership budget covers outstanding receives, queued deliveries and active processing, across all receive pumps. Each batch fits reserved capacity and contains at most ten messages. PostgreSQL lease mode currently uses no additional prefetch.

SQS polling accepts 0–20 seconds. Renewing SQS consumers require a visibility timeout of at least one second and at most twelve hours, and a positive renewal interval shorter than visibility. Negative prefetch is rejected. Set an explicit subsecond renewal interval when using a one-second visibility timeout.

## Ordering

SQS FIFO message groups are serialized in handoff order through settlement and cleanup; different groups can run concurrently. Azure sessions allow one callback per session, with `ConcurrencyCount` concurrent sessions. Other buffered deliveries have no ordering key unless their adapter supplies one. Concurrency one keeps in-memory processing sequential, including retry attempts. Raising concurrency permits overlap and changes completion order.

## Settlement and failures

Success acknowledges the delivery, release requests redelivery, and dead-letter forwards or discards according to the transport. SQS release stops renewal and lets visibility expire; it does not set visibility to zero. PostgreSQL lease release preserves the configured failure redelivery delay and validates the unique lease token. In-memory retains the existing delivery limit and release delay, and discards dead-lettered envelopes.

A failed SQS renewal marks ownership uncertain and rejects subsequent settlement; cleanup stops renewal so the broker can recover the message. Transient SQS topology connection failures use `ConnectionRetry` before consumption starts.

A failed or ambiguous acknowledge or dead-letter operation is not followed by a conflicting second settlement. Cleanup stops renewal and allows broker recovery. PostgreSQL transactional resources are disposed and uncommitted work rolls back. Delivery remains at least once; settlement is not exactly once.

## Startup and shutdown

Schema and topology initialization precede consumption. Producer-only hosts remain supported. Initialization honors startup cancellation, and partially initialized resources are disposed. Unexpected source completion and unhandled faults are retained in `PumpController.Pumps`, which returns a snapshot. Background faults are reported to the host while accepted deliveries drain, so the configured hosted-service failure policy applies.

Graceful shutdown stops intake first, then drains accepted work while retaining delivery ownership and renewal. SDK callbacks and database transactions remain alive through settlement. The host shutdown deadline cancels processing. If a handler ignores cancellation, shutdown reports incomplete work and keeps resources alive until their users return. Adapters must honor cancellation for transport operations.

In-memory shutdown finishes envelopes already held by workers. Envelopes still in the broker channel are not drained during shutdown and have no durability guarantee. Request/reply virtual receive loops retain their transport-specific lifecycle.
