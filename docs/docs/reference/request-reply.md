---
sidebar_position: 2
---

# Request/reply reference

This page documents the InterBus request/reply contracts and runtime semantics.

## `IRequestClient<TRequest, TReply>`

Namespace: `InterBus`

| Member | Description |
|---|---|
| `Task<TReply> RequestAsync(TRequest request, CancellationToken cancellationToken = default)` | Sends a request and waits for a reply of type `TReply` |
| `Task<TReply> RequestAsync(TRequest request, Action<PublishContext<TRequest>> configure, CancellationToken cancellationToken = default)` | Sends a request with additional publish configuration (for example custom headers), then waits for `TReply` |

Behavior:

- Registered as an open-generic **scoped** service
- Sets `InterBusHeaders.ReplyTo` on outgoing request messages
- Uses a temporary reply endpoint created by the transport
- In most transports, that endpoint maps to a single temporary queue per running instance and is reused across request/reply exchanges
- The endpoint/queue lifecycle is transport-managed rather than created and destroyed per request

## `IConsumeContext.ReplyTo`

Namespace: `InterBus`

| Member | Type | Description |
|---|---|---|
| `ReplyTo` | `Uri?` | Reply address from the `InterBusHeaders.ReplyTo` header. Returns `null` if the header is missing or invalid |

In request handlers, use this address to send the reply:

```csharp
if (context.ReplyTo is not null)
    await publisher.SendAsync(context.ReplyTo, reply, cancellationToken);
```

## Relevant headers

| Header | Constant | Purpose |
|---|---|---|
| `interbus-reply-to` | `InterBusHeaders.ReplyTo` | The destination URI where responders should send the reply |
| `interbus-correlation-id` | `InterBusHeaders.CorrelationId` | Correlates related messages; useful for tracing and diagnostics |

## Reply matching semantics

- The request client waits for messages of the expected reply type `TReply`
- Messages of other types received on the temporary reply endpoint are ignored

For implementation guidance, see [Implement request/reply](../how-to-guides/request-reply).
