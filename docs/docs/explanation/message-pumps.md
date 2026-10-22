---
title: How message pumps work
sidebar_position: 4
---

A message pump connects a transport's delivery mechanism to a consumer's middleware and handler. Each consumer has one supervised runtime. The runtime coordinates intake, processing capacity and shutdown, while the transport adapter manages broker or database ownership.

This division lets the transports share dispatch behavior while preserving the resources each delivery needs. Every dispatch gets a fresh DI scope and runs through the same middleware pipeline.

## Intake and processing are separate

Intake acquires messages from the transport. Processing decodes a delivery, runs middleware and the handler, settles the result, then releases delivery resources. A handler returning successfully does not mean the transport has accepted its acknowledgement.

Processing concurrency limits how many deliveries can be processed at once. Receive pump count controls how many buffered acquisition operations can run in parallel. More receive pumps can hide receive latency, but all pumps share the consumer's processing capacity.

For example, an SQS consumer with concurrency eight, three receive pumps and prefetch sixteen can process up to eight deliveries at once. Its shared ownership budget is twenty-four slots across outstanding receive reservations, queued deliveries and active processing. Three pumps do not give it twenty-four processing slots. Prefetch lets intake prepare work while handlers are busy, at the cost of holding more deliveries and maintaining their ownership for longer.

See the [message pump reference](../reference/message-pumps.md#concurrency-and-intake) for defaults, supported pump counts and transport limits.

## Three ways to acquire work

The core supports three execution models because transports provide different ownership and scheduling mechanisms.

| Model | Transports | How work reaches processing |
| --- | --- | --- |
| Buffered polling | SQS, PostgreSQL lease | Core reserves capacity, asks the adapter for a bounded receive, and queues transferred deliveries for processing |
| Scoped polling | PostgreSQL transactional, in-memory | Each worker asks the adapter for one iteration; the adapter invokes processing within that iteration's resource scope |
| SDK callbacks | Azure Service Bus, RabbitMQ | The SDK delivers messages to callbacks that await processing and settlement |

Buffered polling separates acquisition from dispatch. Core owns the receive loop, shared capacity and retry scheduling. The adapter performs one receive and transfers each delivery together with responsibility for its cleanup. Ownership maintenance starts when a delivery is acquired, so queued messages remain protected while waiting for a processing slot.

Scoped polling keeps acquisition and processing in the same asynchronous flow. PostgreSQL transactional workers retain their connection, transaction and ambient transaction context until processing and settlement finish. Each worker can process a delivery independently, so concurrency still permits overlap without buffering database transactions. In-memory uses the same execution model to keep retry attempts sequential when concurrency is one.

Callback transports retain SDK scheduling. Azure processors manage message lock renewal and session scheduling; RabbitMQ uses channel QoS and SDK dispatch concurrency. Core supervises those sources and serializes intake controls. It does not add a polling loop around ordinary callbacks.

## Ownership lasts through settlement

Acquiring a delivery establishes responsibility for it. Depending on the transport, that responsibility may involve a visibility timeout, renewable lease, open transaction, session lock or acknowledgement channel. Those resources must remain usable until settlement and cleanup finish, including while a handler is waiting or intake is paused.

Dispatch chooses an outcome: success, release or dead-letter. Settlement asks the transport to apply that outcome. Release allows redelivery according to the transport's rules; it does not necessarily make the message immediately available.

If acknowledgement fails or its result is uncertain, issuing another, conflicting settlement can make recovery less predictable. The pump preserves the failure and releases resources according to the adapter's recovery rules. Applications must still account for redelivery; successful handler execution does not provide exactly-once delivery.

## Concurrency and ordering

Concurrency allows independent deliveries to overlap. Ordering requires an additional grouping rule: SQS FIFO groups are serialized through settlement and cleanup, while Azure sessions allow one callback per session. Different groups or sessions can progress concurrently. Increasing concurrency therefore does not imply parallel processing within one ordered group.

For deliveries without an ordering key, overlapping handlers can finish in a different order from acquisition. Concurrency one keeps in-memory processing sequential, including retries. The [ordering reference](../reference/message-pumps.md#ordering) lists the transport guarantees.

## Stopping intake and draining work

Graceful shutdown first stops acquiring work, then waits for accepted deliveries to finish. Accepted work includes buffered deliveries waiting to run and callbacks already scheduled by an SDK. Ownership renewal, transactions and settlement resources stay alive during that drain.

An intake operation's cancellation token and an accepted delivery's processing token serve different purposes. Stopping a receive or SDK processor must not automatically cancel handlers that are draining. The host shutdown deadline can cancel processing, but resources remain alive until their users return if a handler ignores cancellation.

Unhandled runtime failures are reported to the host while accepted work drains. A consumer's lifecycle and failure state remain available through `PumpController.Pumps`. See the [startup and shutdown reference](../reference/message-pumps.md#startup-and-shutdown) for the exact behavior and in-memory durability limits.

For adapter interfaces and migration requirements, see [transport adapter contracts](../reference/message-pumps.md#transport-adapter-contracts).
