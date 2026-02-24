# Getting Started with InterBus

This guide walks through integrating InterBus into a .NET application using the standard Microsoft hosting and dependency injection model.

> **Note:** InterBus is currently in early development. Some features described here reflect the intended API and may not yet be fully implemented.

---

## Prerequisites

- .NET 9.0 SDK or later
- Familiarity with `Microsoft.Extensions.DependencyInjection` and `Microsoft.Extensions.Hosting`

---

## Installation

Add the InterBus package to your project:

```bash
dotnet add package InterBus
```

For the in-memory transport (useful for development and testing):

> **Note:** A separate `InterBus.InMemory` NuGet package is planned. Once published, add it with:

```bash
dotnet add package InterBus.InMemory
```

> When building from source, the in-memory transport currently lives under `src/InterBus/InMemory/` as part of the main `InterBus` project — there is no separate `InterBus.InMemory` project yet.

---

## Concepts

Before writing code, it helps to understand the three building blocks:

| Concept | Role |
|---|---|
| **Message** | A plain C# class or record that carries data between components. |
| **Consumer** | A class that implements `IConsumer<T>` and processes a specific message type. |
| **Publisher** | `IMessagePublisher`, injected via DI, used to send messages into the bus. |

InterBus routes a published message to every consumer registered for that message type (Publish-Subscribe). See [architecture.md](architecture.md) for a full explanation of the underlying patterns.

---

## Step 1 — Define a Message

Messages are plain C# types. Using `record` is recommended for immutability:

```csharp
public record OrderPlaced(Guid OrderId, string CustomerEmail, decimal Total);
```

There are no base classes or attributes required.

---

## Step 2 — Implement a Consumer

Implement `IConsumer<T>` for each message type you want to handle:

```csharp
using InterBus;

public class OrderPlacedConsumer : IConsumer<OrderPlaced>
{
    private readonly ILogger<OrderPlacedConsumer> _logger;

    public OrderPlacedConsumer(ILogger<OrderPlacedConsumer> logger)
    {
        _logger = logger;
    }

    public Task Consume(OrderPlaced message)
    {
        _logger.LogInformation(
            "Order {OrderId} placed by {Email} for {Total:C}",
            message.OrderId, message.CustomerEmail, message.Total);

        return Task.CompletedTask;
    }
}
```

Consumer classes are resolved from the DI container on each invocation, so constructor injection of any registered service is supported.

---

## Step 3 — Configure the Host

Register InterBus in your application's `IServiceCollection`. The example below uses a .NET Generic Host:

> **Note:** `AddInterBus`, `AddInMemoryTransport`, and `AddConsumer` are planned DI extension methods. The snippets below reflect the intended API.

```csharp
using InterBus;
using InterBus.InMemory;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddInterBus(bus =>
{
    // Use the in-memory transport (suitable for development and testing)
    bus.AddInMemoryTransport();

    // Register consumers
    bus.AddConsumer<OrderPlacedConsumer>();
});

var host = builder.Build();
await host.RunAsync();
```

### ASP.NET Core

The same registration works inside an ASP.NET Core application:

```csharp
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddInterBus(bus =>
{
    bus.AddInMemoryTransport();
    bus.AddConsumer<OrderPlacedConsumer>();
});

var app = builder.Build();
// ...
await app.RunAsync();
```

---

## Step 4 — Publish a Message

Inject `IMessagePublisher` into any service and call `Publish<T>`:

```csharp
using InterBus;

public class OrderService
{
    private readonly IMessagePublisher _publisher;

    public OrderService(IMessagePublisher publisher)
    {
        _publisher = publisher;
    }

    public async Task PlaceOrderAsync(string customerEmail, decimal total)
    {
        var order = new OrderPlaced(Guid.NewGuid(), customerEmail, total);
        await _publisher.Publish(order);
    }
}
```

`IMessagePublisher` is registered as **scoped** by `AddInterBus`. A scoped lifetime allows the publisher to participate in the same unit-of-work as the calling service — which is required for features like a transactional outbox, where message publication must be tied to the same database transaction as the business operation. Inject it into any scoped or transient service (e.g. an ASP.NET Core controller or a scoped application service) without lifetime concerns. Singleton services should resolve it via `IServiceScopeFactory` if needed.

---

## Multiple Consumers for One Message

Multiple consumers can subscribe to the same message type. InterBus delivers the message to each consumer independently (Publish-Subscribe Channel):

```csharp
public class SendOrderConfirmationEmailConsumer : IConsumer<OrderPlaced>
{
    public Task Consume(OrderPlaced message)
    {
        // Send confirmation email...
        return Task.CompletedTask;
    }
}

public class AuditOrderConsumer : IConsumer<OrderPlaced>
{
    public Task Consume(OrderPlaced message)
    {
        // Write to audit log...
        return Task.CompletedTask;
    }
}
```

Register both consumers at startup:

```csharp
builder.Services.AddInterBus(bus =>
{
    bus.AddInMemoryTransport();
    bus.AddConsumer<SendOrderConfirmationEmailConsumer>();
    bus.AddConsumer<AuditOrderConsumer>();
});
```

---

## Testing with the In-Memory Transport

The in-memory transport makes it straightforward to write integration tests without any external infrastructure. The example below uses a simple `RecordingConsumer<T>` test helper to capture received messages:

```csharp
// Test helper — records every message it receives
public class RecordingConsumer<T> : IConsumer<T> where T : class
{
    public List<T> Received { get; } = new();

    public Task Consume(T message)
    {
        Received.Add(message);
        return Task.CompletedTask;
    }
}

public class OrderServiceTests
{
    [Fact]
    public async Task PlaceOrder_PublishesOrderPlacedMessage()
    {
        var builder = Host.CreateApplicationBuilder();

        builder.Services.AddInterBus(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddConsumer<RecordingConsumer<OrderPlaced>>();
        });

        var host = builder.Build();
        var services = host.Services;

        var publisher = services.GetRequiredService<IMessagePublisher>();
        await publisher.Publish(new OrderPlaced(Guid.NewGuid(), "test@example.com", 99.99m));

        var consumer = services.GetRequiredService<RecordingConsumer<OrderPlaced>>();
        Assert.Single(consumer.Received);
    }
}
```

---

## Next Steps

- Read [architecture.md](architecture.md) for a deeper understanding of the component model and Enterprise Integration Patterns used.
- Browse [design notes](design.md) for open questions and future directions.
- Explore planned RabbitMQ and AWS SQS transports for production deployments.
