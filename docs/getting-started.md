# Getting Started with InterBus

## Installation

Add the NuGet packages to your project:

```
dotnet add package InterBus
dotnet add package InterBus.InMemory
```

## Basic Usage

### 1. Define a message

```csharp
public record OrderPlaced(Guid OrderId, string CustomerEmail, decimal Total);
```

### 2. Implement a consumer

```csharp
public class OrderPlacedConsumer : IConsumer<OrderPlaced>
{
    public Task Consume(OrderPlaced message, CancellationToken cancellationToken = default)
    {
        Console.WriteLine($"Order {message.OrderId} placed by {message.CustomerEmail}");
        return Task.CompletedTask;
    }
}
```

### 3. Register with DI

```csharp
builder.Services.AddInterBus(bus =>
{
    bus.AddInMemoryTransport();
    bus.AddConsumer<OrderPlacedConsumer>();
});
```

### 4. Publish a message

```csharp
public class OrderService
{
    private readonly IMessagePublisher _publisher;

    public OrderService(IMessagePublisher publisher) => _publisher = publisher;

    public async Task PlaceOrderAsync(Order order)
    {
        // ... business logic ...
        await _publisher.Publish(new OrderPlaced(order.Id, order.Email, order.Total));
    }
}
```

## Publish vs Send

- **`Publish<T>`** — sends to the topic; all registered consumers for `T` receive the message (fan-out).
- **`Send<TConsumer, T>`** — sends directly to a specific consumer's queue; no other consumer receives it.

```csharp
// Fan-out to all consumers of OrderPlaced
await publisher.Publish(new OrderPlaced(...));

// Direct send to a specific consumer only
await publisher.Send<AuditConsumer, OrderPlaced>(new OrderPlaced(...));
```
