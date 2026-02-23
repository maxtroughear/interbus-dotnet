# InterBus Architecture

InterBus is an application framework for asynchronous, message-driven .NET applications. Its design is grounded in [Enterprise Integration Patterns (EIP)](https://www.enterpriseintegrationpatterns.com/) and the conventions of the Microsoft .NET ecosystem.

---

## Table of Contents

1. [Design Philosophy](#design-philosophy)
2. [Core EIP Concepts](#core-eip-concepts)
3. [Component Overview](#component-overview)
4. [Message Flow](#message-flow)
5. [Project Structure](#project-structure)
6. [Transport Model](#transport-model)
7. [Microsoft Ecosystem Integration](#microsoft-ecosystem-integration)
8. [Design Constraints and Goals](#design-constraints-and-goals)

---

## Design Philosophy

InterBus follows the **dumb pipes, smart endpoints** principle from EIP:

> _"Keep the middleware simple. Put the domain logic in the endpoints."_

The framework is responsible for:
- Reliable message delivery between endpoints via **channels** (topics and queues).
- Transparent routing from message type to the appropriate channel.
- Lifecycle management of consumers via the .NET hosted-service model.

The application code (consumers) is responsible for:
- All business logic triggered by a message.
- Idempotency and error handling within the consumer.

---

## Core EIP Concepts

The table below maps EIP terminology to InterBus abstractions.

| EIP Pattern | Description | InterBus Abstraction |
|---|---|---|
| [Message](https://www.enterpriseintegrationpatterns.com/patterns/messaging/Message.html) | Data transmitted between endpoints. | Plain C# class or record (`T`) |
| [Message Channel](https://www.enterpriseintegrationpatterns.com/patterns/messaging/MessageChannel.html) | A named pipe through which messages flow. | `Topic` |
| [Point-to-Point Channel](https://www.enterpriseintegrationpatterns.com/patterns/messaging/PointToPointChannel.html) | Exactly one consumer receives each message. | `Queue` (per consumer, per topic) |
| [Publish-Subscribe Channel](https://www.enterpriseintegrationpatterns.com/patterns/messaging/PublishSubscribeChannel.html) | All subscribed consumers receive each message. | Topic with multiple subscriber queues |
| [Message Endpoint](https://www.enterpriseintegrationpatterns.com/patterns/messaging/MessageEndpoint.html) | An application component connected to a channel. | `IConsumer<T>` / `IMessagePublisher` |
| [Message Router](https://www.enterpriseintegrationpatterns.com/patterns/messaging/MessageRouter.html) | Directs messages to the appropriate channel based on content or type. | Type-to-topic routing map |
| [Service Activator](https://www.enterpriseintegrationpatterns.com/patterns/messaging/MessagingAdapter.html) | Activates a service (consumer) when a message arrives. | Message Pump (background hosted service) |
| [Dead Letter Channel](https://www.enterpriseintegrationpatterns.com/patterns/messaging/DeadLetterChannel.html) | Holds messages that could not be delivered or processed. | Planned |
| [Message Store](https://www.enterpriseintegrationpatterns.com/patterns/messaging/MessageStore.html) | Persists messages for auditing or replay. | Planned |

---

## Component Overview

### `IMessagePublisher` — Message Endpoint (Publisher)

```csharp
public interface IMessagePublisher
{
    Task Publish<T>(T message);
}
```

`IMessagePublisher` is the primary entry point for sending messages into the bus. It represents the **publisher side** of a Message Endpoint. Callers publish a typed message without any knowledge of which consumers will receive it or via which transport.

This interface is registered with the DI container and injected into application services.

---

### `IConsumer<T>` — Message Endpoint (Consumer)

```csharp
public interface IConsumer<T> : IConsumer where T : class
{
    Task Consume(T message);
}
```

`IConsumer<T>` is the **consumer side** of a Message Endpoint. Each implementation handles messages of a single type `T`. A consumer can be thought of as a **Service Activator**: it is invoked by the Message Pump when a message of type `T` is available on the consumer's queue.

Multiple consumers may implement `IConsumer<T>` for the same message type, enabling **Publish-Subscribe** fan-out.

The non-generic `IConsumer` marker interface is used for consumer discovery and registration.

---

### Topic — Publish-Subscribe Channel

A **Topic** is the named message channel associated with a message type. When a message is published, it is placed onto the topic corresponding to its type. The topic then fans the message out to each registered subscriber queue.

Topics are registered explicitly at startup, either by message type (using the fully-qualified type name as the channel name) or with an explicit name.

---

### Queue — Point-to-Point Channel

A **Queue** is a buffer attached to a topic for a specific consumer. By default, queues are scoped at the **consumer × message type** level: each `IConsumer<T>` registration produces one dedicated queue on the corresponding topic.

This means a single consumer class that implements multiple `IConsumer<T>` interfaces (e.g. `IConsumer<OrderPlaced>` and `IConsumer<OrderShipped>`) receives **one queue per message type**, not one shared queue for all its messages.

**Comparison of queue granularity models:**

| Approach | When to use | Pros | Cons |
|---|---|---|---|
| One queue per consumer × message type *(default)* | Independent message types with no ordering constraint across types. | Simple routing — no type discrimination inside the queue. Independent backpressure and throughput per message type. Aligns naturally with the type-to-topic routing model. | Multiple message types targeting the same consumer are processed concurrently and independently — not in global arrival order. |
| One queue per consumer class *(FIFO mode)* | A consumer must process all its message types in strict global arrival order (e.g. `OrderPlaced` then `OrderShipped` for the same order). | Messages interleave across types in arrival order. Back-pressure is applied uniformly across all message types the consumer handles, which is fundamental to correct FIFO processing when types share a dependency. | Requires the Message Pump to carry type metadata alongside the message payload and perform type dispatch after dequeue. |

#### FIFO mode: per-consumer-class queue

In a FIFO scenario, the per-consumer-class queue model is not only acceptable — the shared backpressure behaviour is actually required for correctness. If `OrderPlaced` and `OrderShipped` messages for the same order must be processed in arrival order, separate per-type queues would allow `OrderShipped` to be dispatched before the preceding `OrderPlaced` had been processed, violating ordering guarantees.

#### Supporting both models

Supporting both is feasible and the added complexity is moderate. The main pieces of work are:

1. **Consumer registration opt-in.** A flag or attribute on the consumer registration (`AddConsumer<T>(ConsumerQueueMode.Fifo)`) signals that the consumer should use a single shared queue.
2. **Type envelope in the shared queue.** The single queue must store a `(Type, object)` pair so the pump knows which `IConsumer<T>` method to dispatch to after dequeue. This is the only place type inspection is needed and it is confined to the pump.
3. **Message Pump branching.** The pump already needs to know its queue mode to decide whether to create one queue or many per consumer. The dispatch path diverges only after dequeue: per-type queues go straight to `Consume(message)`; the shared queue first resolves the correct `IConsumer<T>` from DI.
4. **Transport mapping.** For external transports (RabbitMQ, SQS) the FIFO mode typically maps to a single named queue bound to multiple routing keys or a FIFO SQS queue, rather than multiple queues. The transport layer must express this distinction.

The core API surface change is small (one extra parameter on `AddConsumer`). The implementation overhead is mainly in the pump and transport mapping layers, and is self-contained enough that it does not affect consumers that use the default per-type mode.

> Queue mode configuration is planned and not yet implemented. The default per-(consumer × message type) model is the current implementation target.

The Message Pump reads messages from a consumer's queue(s) and dispatches them to the consumer, ensuring point-to-point semantics within the fan-out.

---

### Message Pump — Service Activator

The Message Pump is a background hosted service (`IHostedService`) that connects a transport to its registered consumers. It reads messages from a consumer's queue and invokes the corresponding `IConsumer<T>.Consume` method.

**Transport-agnostic interface, transport-specific implementation.** The pump contract (read a message, dispatch to a consumer, handle errors) is generic and defined in the core library. However, each physical transport has fundamentally different delivery semantics — RabbitMQ is push-based via AMQP channels, AWS SQS requires polling, and the in-memory transport uses .NET `Channel<T>`. Therefore, each transport package provides its own Message Pump implementation that satisfies the core interface. Application code never interacts with the pump directly; it is wired up automatically when a transport is configured.

The pump is responsible for:
- Concurrent or sequential dispatch (configurable).
- Error isolation — a failing consumer does not affect other consumers.
- Graceful shutdown in response to `CancellationToken`.

> The Message Pump is planned and not yet fully implemented.

---

### Transport — Message Channel Infrastructure

A **Transport** provides the physical implementation of topics and queues. InterBus supports a pluggable transport model:

| Transport | Status | Description |
|---|---|---|
| In-Memory | In progress | `System.Threading.Channels`-backed topics and queues. Suitable for testing and single-process applications. |
| RabbitMQ | Planned | AMQP-based durable messaging via the RabbitMQ .NET client. |
| AWS SQS | Planned | Cloud-native queue-based messaging via the AWS SDK for .NET. |

Each transport implements the core channel abstractions, allowing application code to remain transport-agnostic.

---

## Message Flow

The following describes the flow of a message from publication to consumption:

```
Application Code
     │
     │  publisher.Publish(new OrderPlaced(...))
     ▼
IMessagePublisher
     │
     │  Route: typeof(OrderPlaced) → topic name
     ▼
Message Router (type-to-topic map)
     │
     │  Fan-out to subscriber queues
     ▼
Topic (Publish-Subscribe Channel)
     ├──► Queue [OrderPlacedConsumer]  ──► Message Pump ──► OrderPlacedConsumer.Consume(...)
     └──► Queue [AuditConsumer]        ──► Message Pump ──► AuditConsumer.Consume(...)
```

1. The application publishes a typed message via `IMessagePublisher`.
2. The **Message Router** resolves the topic for the message type.
3. The **Topic** copies the message to each registered subscriber queue.
4. The **Message Pump** reads from each queue and dispatches to the corresponding consumer.
5. The **Consumer** processes the message and returns.

---

## Project Structure

```
src/
└── InterBus/
    ├── InterBus.csproj
    │
    ├── IMessagePublisher.cs        # Publisher Message Endpoint abstraction
    ├── IConsumer.cs                # Consumer Message Endpoint abstraction
    │
    └── InMemory/                   # In-Memory Transport implementation
        ├── InMemoryTransport.cs    # Channel-backed topic/queue implementation
        └── InMemoryMessagePublisher.cs  # IMessagePublisher for the in-memory transport
```

### Planned structure

Each physical transport is a **separate project and NuGet package** that depends on the core `InterBus` package. This keeps the core library minimal and allows consuming applications to include only the transport(s) they need.

```
src/
├── InterBus/                        # Core abstractions only (IMessagePublisher, IConsumer<T>)
│   ├── InterBus.csproj
│   ├── IMessagePublisher.cs
│   ├── IConsumer.cs
│   └── Extensions/
│       └── ServiceCollectionExtensions.cs  # AddInterBus() core DI helpers
│
├── InterBus.InMemory/               # In-memory transport package
│   ├── InterBus.InMemory.csproj
│   └── ...                          # Channel-backed topics, queues, and Message Pump
│
├── InterBus.RabbitMq/               # RabbitMQ transport package
│   ├── InterBus.RabbitMq.csproj
│   └── ...                          # AMQP topics, queues, and push-based Message Pump
│
└── InterBus.AwsSqs/                 # AWS SQS transport package
    ├── InterBus.AwsSqs.csproj
    └── ...                          # SQS queues and polling-based Message Pump
```

---

## Transport Model

Transports are pluggable and implement the internal channel abstractions (Topic, Queue, and Message Pump). Application code interacts only with `IMessagePublisher` and `IConsumer<T>`, remaining entirely transport-agnostic.

Each transport is shipped as an independent NuGet package (`InterBus.InMemory`, `InterBus.RabbitMq`, `InterBus.AwsSqs`, etc.) that references the core `InterBus` package. This ensures the core library stays lean and applications only pull in the dependencies of the transport they use.

| Transport | Package | Status | Description |
|---|---|---|---|
| In-Memory | `InterBus.InMemory` | In progress | `System.Threading.Channels`-backed topics and queues. Suitable for testing and single-process applications. |
| RabbitMQ | `InterBus.RabbitMq` | Planned | AMQP-based durable messaging via the RabbitMQ .NET client. Push-based Message Pump. |
| AWS SQS | `InterBus.AwsSqs` | Planned | Cloud-native queue-based messaging via the AWS SDK for .NET. Polling-based Message Pump. |

At startup, exactly one transport is configured per bus instance. The transport is responsible for:
- Managing the lifecycle of topics and queues.
- Serializing and deserializing messages (using `System.Text.Json` by default).
- Providing the transport-specific Message Pump implementation.

---

## Microsoft Ecosystem Integration

InterBus is designed to integrate naturally with the standard .NET hosting and DI model:

| Concern | Integration |
|---|---|
| Dependency Injection | `IServiceCollection` extension methods (`AddInterBus`) |
| Application Hosting | `IHostedService` for the Message Pump background service |
| Configuration | `IConfiguration` / `IOptions<T>` for transport settings |
| Logging | `ILogger<T>` injected into the Message Pump and consumers |
| Cancellation | `CancellationToken` propagated through publish and consume paths |
| Serialization | `System.Text.Json` (source-generated, AOT-compatible) |

---

## Design Constraints and Goals

| Goal | Approach |
|---|---|
| No runtime reflection | Source generators for consumer registration and type mapping |
| Native AOT compatibility | Source-generated serialization; avoid reflection-based APIs |
| Testability | In-memory transport usable in unit and integration tests without external infrastructure |
| Explicit over implicit | Consumers and topics are registered explicitly at startup |
| Transport portability | Application code depends only on `IMessagePublisher` and `IConsumer<T>` |
