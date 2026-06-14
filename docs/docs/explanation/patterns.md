---
sidebar_position: 2
---

# InterBus Concepts

This page defines the core messaging concepts and links to deeper reliability pattern docs.

## Core messaging concepts

| Concept | Definition |
|---|---|
| Message | A serializable contract (usually a .NET type) that represents business intent or state change |
| Consumer | A component that handles a specific message type via `IConsumer<T>` |
| Publisher | A component that emits messages via `IPublisher` |
| Topic | A publish/subscribe channel where each published message is fanned out to all subscribed consumer queues |
| Queue | A point-to-point channel that delivers a message to one target consumer pipeline |
| Routing convention | Deterministic rules that map message and consumer types to topic and queue names |
| Message pump | The receive loop that reads messages from queues and dispatches them to consumers |

## Delivery and processing concepts

| Concept | Definition |
|---|---|
| Publish | `Publish<T>(message)` sends a message to a topic so all subscribers can process it |
| Send | `Send<TConsumer, T>(message)` sends a message directly to one consumer queue |
| Consumer middleware | Ordered processing steps around consumer execution (for cross-cutting concerns like retries, logging, and policies) |
| Retry | A policy that re-attempts failed message handling before giving up |
| Dead-letter | A terminal destination for messages that cannot be processed successfully |
| Idempotency | The ability to process duplicate deliveries safely without changing the final outcome |

## Reliability patterns and consistency concepts

| Concept | Definition |
|---|---|
| Outbox | A producer-side reliability pattern for atomic domain write + message buffering, then asynchronous delivery |
| Inbox | A consumer-side reliability pattern for duplicate-detection and safe re-delivery handling |
| Saga | A long-running workflow that coordinates multiple message exchanges and state transitions |
| Saga state | Persisted state associated with a saga instance across message boundaries |
| Scheduler | A service that arranges delayed or time-based message publication/dispatch |
| Transport | The broker implementation responsible for moving bytes between publishers and consumers (for example in-memory, SQS/SNS, Service Bus, RabbitMQ, PostgreSQL) |

## Pattern deep dives

- [Outbox pattern](./patterns/outbox)
- [Inbox pattern](./patterns/inbox)
