---
sidebar_position: 4
---

# Implement request/reply

Use request/reply when one component needs a direct response from another component, and you want that exchange over InterBus instead of an in-process call.

## When to use request/reply vs publish/send

- Use **request/reply** for one-to-one interactions where the caller must await a typed response
- Use **publish** for fan-out events where no direct response is expected
- Use **send** for one-way point-to-point commands

## 1. Define request and reply contracts

```csharp
public sealed record GetOrderStatus(Guid OrderId);
public sealed record OrderStatusReply(Guid OrderId, string Status);
```

## 2. Register InterBus and the responder consumer

`IRequestClient<TRequest, TReply>` is registered by InterBus as an open generic scoped service.

```csharp
using InterBus.Extensions;
using InterBus.Generated;
using InterBus.InMemory.Extensions;

builder.Services.AddInterBus(bus =>
{
    bus.AddInMemoryTransport();
    bus.AddConsumer<GetOrderStatusConsumer>();
});
```

## 3. Reply from the consumer using `context.ReplyTo`

The requester sets `InterBusHeaders.ReplyTo` automatically. In consumers, read it through `IConsumeContext.ReplyTo` and send the reply message to that address.

```csharp
public sealed class GetOrderStatusConsumer(IPublisher publisher) : IConsumer<GetOrderStatus>
{
    public async Task Consume(
        GetOrderStatus message,
        IConsumeContext context,
        CancellationToken cancellationToken = default)
    {
        if (context.ReplyTo is null) return;

        var reply = new OrderStatusReply(message.OrderId, "Shipped");
        await publisher.SendAsync(context.ReplyTo, reply, cancellationToken);
    }
}
```

## 4. Send requests from application code

Inject and use `IRequestClient<TRequest, TReply>` in a scoped service.

```csharp
public sealed class OrderQueryService(
    IRequestClient<GetOrderStatus, OrderStatusReply> client)
{
    public Task<OrderStatusReply> GetStatusAsync(
        Guid orderId,
        CancellationToken cancellationToken = default)
    {
        return client.RequestAsync(new GetOrderStatus(orderId), cancellationToken);
    }
}
```

You can also attach request headers via the configure delegate:

```csharp
var reply = await client.RequestAsync(
    new GetOrderStatus(orderId),
    ctx => ctx.Headers["x-trace-source"] = "orders-api",
    cancellationToken);
```

## 5. Apply cancellation/timeout at the caller

Use a cancellation token (optionally with a timeout) to bound how long the caller waits for a reply.

```csharp
using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
cts.CancelAfter(TimeSpan.FromSeconds(5));

var reply = await client.RequestAsync(new GetOrderStatus(orderId), cts.Token);
```

## Behavior notes

- Request messages are published and include an automatically generated reply address
- Transports typically create one temporary reply queue per running app instance and reuse it for request/reply exchanges to avoid per-request infrastructure churn
- The request client waits for a reply of the expected `TReply` type
- Non-matching reply message types are ignored by the temporary reply endpoint

For API-level details, see [Request/reply reference](../reference/request-reply).
