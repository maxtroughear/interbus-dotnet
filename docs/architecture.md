# InterBus Architecture

InterBus is a .NET message bus library built around the **Enterprise Integration Patterns (EIP)**.

## EIP Pattern Mapping

| EIP Pattern | InterBus Concept |
|---|---|
| Message Endpoint (consumer) | `IConsumer<T>` |
| Message Endpoint (publisher) | `IMessagePublisher` |
| Publish-Subscribe Channel | Topic (fan-out to all subscriber queues) |
| Point-to-Point Channel | Queue (one consumer per queue) |
| Message Router | `RoutingConventions` (type → topic/queue name) |
| Service Activator / Message Pump | `InMemoryMessagePump` (BackgroundService) |

## Components

### Core (`InterBus`)

- **`IConsumer<T>`** — Implement to handle messages of type `T`. Activated per-message from a fresh DI scope.
- **`IMessagePublisher`** — Inject to publish messages. Registered as scoped.
- **`IInterBusTransport`** — Internal contract between core and transport packages.
- **`ConsumerDescriptor`** — Describes a consumer with its pre-compiled dispatch delegate.
- **`InterBusOptions`** — Holds all registered `ConsumerDescriptor` instances.
- **`IInterBusBuilder`** — Fluent builder returned by `AddInterBus(...)`.

### InMemory Transport (`InterBus.InMemory`)

- **`InMemoryBroker`** — Singleton that owns all `InMemoryTopic` and `InMemoryQueue` instances.
- **`InMemoryTopic`** — Fans out payloads to all subscriber queues (`Channel<byte[]>`-backed).
- **`InMemoryQueue`** — `Channel<byte[]>`-backed queue for a single consumer/message-type pair.
- **`InMemoryMessagePump`** — `BackgroundService` that reads each queue and dispatches messages.

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
   │                                                  (BackgroundService)
   │                                                         │
   │                                               per-message DI scope
   │                                                         │
   │                                               IConsumer<T>.Consume()
```
