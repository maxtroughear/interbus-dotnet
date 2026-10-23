---
sidebar_position: 3
---

# InterBus Architecture

InterBus is a .NET message bus library built around the **Enterprise Integration Patterns (EIP)**.

## Abstract Solution Architecture

InterBus composes the solution from modular layers so application code stays transport-agnostic:

- **Core contracts and pipeline (`InterBus`)**
  - Message contracts and handlers (`IConsumer<T>`, `IPublisher`)
  - Dispatching, middleware, routing, dead-letter and retry primitives
  - Saga and scheduling abstractions
- **Transport implementations (separate packages)**
  - Broker-specific send/publish and receive pumps
  - Topology and delivery behavior mapped to each provider
- **Integration and infrastructure packages**
  - Persistence-backed features (outbox, saga repositories)
  - Scheduling providers
  - Source generation for compile-time registration

This architecture allows swapping transports or infrastructure integrations without changing consumer/message code.

## EIP Pattern Mapping

| EIP Pattern | InterBus Concept |
|---|---|
| Message Endpoint (consumer) | `IConsumer<T>` |
| Message Endpoint (publisher) | `IPublisher` |
| Publish-Subscribe Channel | Topic (fan-out to all subscriber queues) |
| Point-to-Point Channel | Queue (one consumer per queue) |
| Message Router | `RoutingConventions` (type → topic/queue name) |
| Service Activator / Message Pump | Shared consumer runtime with a transport source |

## Components

### Core (`InterBus`)

- **`IConsumer<T>`** — Implement to handle messages of type `T`. Activated per-message from a fresh DI scope.
- **`IPublisher`** — Inject to publish messages. Registered as scoped.
- **`IInterBusTransport`** — Internal contract between core and transport packages.
- **`ConsumerDescriptor`** — Describes a consumer with its pre-compiled dispatch delegate.
- **`InterBusOptions`** — Holds all registered `ConsumerDescriptor` instances.
- **`IInterBusBuilder`** — Fluent builder returned by `AddInterBus(...)`.

### InMemory Transport (`InterBus` / `InterBus.InMemory` namespace)

- **`InMemoryBroker`** — Singleton that owns all `InMemoryTopic` and `InMemoryQueue` instances.
- **`InMemoryTopic`** — Fans out payloads to all subscriber queues (`Channel<byte[]>`-backed).
- **`InMemoryQueue`** — `Channel<byte[]>`-backed queue for a single consumer/message-type pair.
- **`InMemoryMessagePump`** — Scoped polling source that reads a queue and invokes processing within each iteration. The shared consumer runtime supervises it and controls concurrency.

Read [How message pumps work](./message-pumps.md) for the shared runtime, transport execution models and delivery ownership.

## Currently Supported Transports

- **`InterBus`** via `AddInMemoryTransport(...)`
- **`InterBus.AmazonSqs`** (SNS/SQS) via `AddAmazonSqsTransport(...)`
- **`InterBus.AzureServiceBus`** via `AddAzureServiceBusTransport(...)`
- **`InterBus.RabbitMQ`** via `AddRabbitMqTransport(...)`
- **`InterBus.PostgreSQL`** via `AddPostgreSqlTransport(...)`

## Currently Supported Integrations

- **EF Core (`InterBus.EntityFrameworkCore`)**
  - Outbox registration via `UseOutbox<TDbContext>(...)`
  - Saga repository via `UseEfCoreRepository<TSaga, TState, TDbContext>(...)`
- **DynamoDB (`InterBus.DynamoDB`)**
  - Saga repository via `UseDynamoDbRepository<TSaga, TState>(...)`
- **Scheduling providers**
  - TickerQ via `AddTickerQScheduler()`
  - Azure Service Bus via `AddAzureServiceBusScheduler()`
  - Hangfire via `AddHangfireScheduler()`
  - Amazon EventBridge scheduler via `AddEventBridgeScheduler(...)`
- **Compile-time registration**
  - Source generator package: `InterBus.SourceGenerator`

## Feature Areas

- Publish/subscribe and point-to-point messaging (`Publish` / `Send`)
- Consumer middleware composition
- Retry middleware (`UseRetryMiddleware(...)`)
- Saga orchestration and state lifecycle (persist, complete, delete)
- [Scheduling](../how-to-guides/scheduling.md) abstraction (`AddScheduling(...)`, `IScheduler`)
- Durable outbox support with EF Core

## Routing Conventions

- **Topic name**: `messageType.FullName` (e.g. `"MyApp.OrderPlaced"`)
- **Queue name**: `"{consumerType.FullName}/{messageType.FullName}"` (e.g. `"MyApp.OrderPlacedConsumer/MyApp.OrderPlaced"`)

## Publish vs Send

- **`Publish<T>(message)`** — Writes to the topic → fans out to **all** subscriber queues.
- **`Send<TConsumer, T>(message)`** — Writes directly to the specific consumer's queue (bypasses topic fan-out).

## Message Flow

```
Publisher                   Topic                    Queue (per consumer)
   │                          │                              │
   │── Publish<T>(msg) ──►  FanOut ──► WriteAsync ──►  Channel<byte[]>
   │                                                         │
   │                                               InMemoryMessagePump
   │                                              shared consumer runtime
   │                                                         │
   │                                               per-message DI scope
   │                                                         │
   │                                               IConsumer<T>.Consume()
```
