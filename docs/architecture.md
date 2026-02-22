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

A **Queue** is a per-consumer buffer attached to a topic. Each `IConsumer<T>` registration results in a dedicated queue on the topic. The Message Pump reads messages from this queue and dispatches them to the consumer, ensuring point-to-point semantics within the fan-out.

---

### Message Pump — Service Activator

The Message Pump is a background hosted service (`IHostedService`) that connects the transport layer to registered consumers. It reads messages from a consumer's queue and invokes the corresponding `IConsumer<T>.Consume` method.

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

### Planned additions

```
src/
└── InterBus/
    ├── Extensions/
    │   └── ServiceCollectionExtensions.cs  # AddInterBus() DI registration helpers
    ├── Pump/
    │   └── MessagePump.cs                  # Background Service Activator
    ├── RabbitMq/                           # RabbitMQ transport
    └── AwsSqs/                             # AWS SQS transport
```

---

## Transport Model

Transports are pluggable and implement the internal channel abstractions (Topic, Queue). Application code interacts only with `IMessagePublisher` and `IConsumer<T>`, remaining entirely transport-agnostic.

At startup, exactly one transport is configured per bus instance. The transport is responsible for:
- Managing the lifecycle of topics and queues.
- Serializing and deserializing messages (using `System.Text.Json` by default).
- Providing the backing channel for the Message Pump.

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
