# InterBus

Interoperable asynchronous messaging-based application framework for .NET.

InterBus is a lightweight, extensible messaging framework that brings [Enterprise Integration Patterns](https://www.enterpriseintegrationpatterns.com/) into the .NET ecosystem. It integrates natively with the Microsoft dependency injection stack (`Microsoft.Extensions.DependencyInjection`) and is designed for high-performance, AOT-compatible applications.

## Overview

InterBus models messaging using the core vocabulary from Enterprise Integration Patterns (EIP):

| EIP Concept | InterBus Abstraction |
|---|---|
| Message | Plain C# class or record (`T`) |
| Message Channel | `Topic` / `Queue` |
| Publish-Subscribe Channel | `IMessagePublisher.Publish<T>` → Topic fan-out |
| Message Endpoint (publisher) | `IMessagePublisher` |
| Message Endpoint (consumer) | `IConsumer<T>` |
| Service Activator | Message Pump (background service) |
| Message Router | Type-to-topic routing |

## Features

- **Publish-Subscribe messaging** — Decouple producers from consumers via typed message channels.
- **Type-safe routing** — Message types are mapped to topics automatically; no magic strings required.
- **Microsoft DI first** — Register consumers and transports with the standard `IServiceCollection` API.
- **Multiple transport support** — In-memory transport included; RabbitMQ and AWS SQS transports planned.
- **No reflection at runtime** — Designed around source generators and generic code for Native AOT compatibility.
- **Explicit consumer registration** — Consumers are registered at startup for predictable, traceable message flow.

## Quick Start

> **Note:** InterBus is currently in early development. The API shown below reflects the intended design.

### 1. Define a message

```csharp
public record OrderPlaced(Guid OrderId, decimal Total);
```

### 2. Implement a consumer

```csharp
public class OrderPlacedConsumer : IConsumer<OrderPlaced>
{
    public Task Consume(OrderPlaced message)
    {
        Console.WriteLine($"Order {message.OrderId} placed for {message.Total:C}");
        return Task.CompletedTask;
    }
}
```

### 3. Register with Microsoft DI

```csharp
var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddInterBus(bus =>
{
    bus.AddInMemoryTransport();
    bus.AddConsumer<OrderPlacedConsumer>();
});

var host = builder.Build();
await host.RunAsync();
```

### 4. Publish a message

```csharp
public class OrderService(IMessagePublisher publisher)
{
    public async Task PlaceOrder(decimal total)
    {
        await publisher.Publish(new OrderPlaced(Guid.NewGuid(), total));
    }
}
```

## Project Structure

```
src/
├── InterBus/                    # Core abstractions (IMessagePublisher, IConsumer<T>)
├── InterBus.InMemory/           # In-memory transport package (planned — currently src/InterBus/InMemory/)
├── InterBus.RabbitMq/           # RabbitMQ transport package (planned)
└── InterBus.AwsSqs/             # AWS SQS transport package (planned)
```

See [docs/architecture.md](docs/architecture.md) for a full description of the project structure and design.

## Documentation

- [Architecture](docs/architecture.md) — Component design and Enterprise Integration Patterns mapping.
- [Getting Started](docs/getting-started.md) — Step-by-step guide for integrating InterBus into a .NET application.
- [Design Notes](docs/design.md) — Internal design notes and open questions.

## Contributing

Contributions are welcome. Please open an issue or pull request on [GitHub](https://github.com/maxtroughear/interbus-dotnet).

## License

MIT — see [LICENSE](LICENSE).
