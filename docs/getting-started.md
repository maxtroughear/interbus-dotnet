# Getting Started with InterBus

## Installation

Add the core package. For local development and tests, the in-memory transport is the easiest place to start:

```bash
dotnet add package InterBus
```

The in-memory transport extensions and source generator support are included through `InterBus`.

## Basic usage

### 1. Define a message

```csharp
public record OrderPlaced(Guid OrderId, string CustomerEmail, decimal Total);
```

### 2. Implement a consumer

```csharp
public sealed class OrderPlacedConsumer : IConsumer<OrderPlaced>
{
    public Task Consume(
        OrderPlaced message,
        IConsumeContext context,
        CancellationToken cancellationToken = default)
    {
        Console.WriteLine(
            $"Order {message.OrderId} placed by {message.CustomerEmail} for {message.Total:C}");

        return Task.CompletedTask;
    }
}
```

### 3. Register InterBus

```csharp
using InterBus.Extensions;
using InterBus.Generated;
using InterBus.InMemory.Extensions;

builder.Services.AddInterBus(bus =>
{
    bus.AddInMemoryTransport();
    bus.AddConsumer<OrderPlacedConsumer>();
});
```

`AddConsumer<TConsumer>()` is generated at compile time, so you should import `InterBus.Generated`.

### 4. Publish a message

```csharp
public sealed class OrderService(IPublisher publisher)
{
    public Task PlaceOrderAsync(Order order, CancellationToken cancellationToken = default)
    {
        return publisher.PublishAsync(
            new OrderPlaced(order.Id, order.Email, order.Total),
            cancellationToken);
    }
}
```

### 5. Run the host

InterBus transports use hosted background services. In ASP.NET Core, `app.Run()` starts them automatically. In console or worker-style apps, start the host before publishing:

```csharp
var host = builder.Build();

await host.StartAsync();

using var scope = host.Services.CreateScope();
var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
await publisher.PublishAsync(new OrderPlaced(Guid.NewGuid(), "alice@example.com", 42m));

await host.StopAsync();
```

## Publish vs send

- **`PublishAsync<T>`** sends to the topic for `T`, so every registered consumer for that message type receives a copy
- **`SendAsync<TConsumer, T>`** sends directly to one consumer queue and bypasses topic fan-out

```csharp
await publisher.PublishAsync(new OrderPlaced(...));
await publisher.SendAsync<AuditConsumer, OrderPlaced>(new OrderPlaced(...));
```

## Next steps

- For a deeper architectural overview, see `docs/architecture.md`
- For runnable examples, see the projects under `samples/`
