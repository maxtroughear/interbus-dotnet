---
sidebar_position: 2
---

# Writing consumers

This guide covers practical guidance for implementing reliable `IConsumer<T>` handlers in InterBus.

## Keep handlers focused

Treat each consumer as a single message handler with a clear responsibility:

- Validate required inputs early
- Execute one cohesive business action
- Delegate shared business logic to domain/application services
- Return once the action is complete

Keeping handlers small makes them easier to reason about, test, and evolve.

## Handle idempotency explicitly

Message delivery can be retried, and duplicate deliveries can occur in distributed systems. Consumers should be safe to run more than once for the same logical event.

Common patterns:

- Store and check a processed message ID before applying side effects (this is included in some integrations, see EF Core)
- Use upsert/merge semantics instead of insert-only where appropriate
- Design external calls (email, webhooks, payments) with deduplication keys

## Respect cancellation and async flow

`Consume` receives a `CancellationToken`; pass it through to all async dependencies (database calls, HTTP calls, and other I/O). Avoid blocking calls or fire-and-forget work inside consumers.

```csharp
public sealed class OrderPlacedConsumer(IOrderService orders) : IConsumer<OrderPlaced>
{
    public Task Consume(
        OrderPlaced message,
        IConsumeContext context,
        CancellationToken cancellationToken = default)
    {
        return orders.ProcessOrderAsync(message.OrderId, cancellationToken);
    }
}
```

## Fail loudly and intentionally

Do not swallow exceptions. If a message cannot be processed, let the failure surface so retry/dead-letter behavior can do its job.

Use clear exception messages and structured logs that include stable identifiers (for example `OrderId`, `CustomerId`, or `CorrelationId`) so failed deliveries can be diagnosed quickly.

## Minimize side effects and ordering assumptions

Consumers should avoid relying on global ordering across messages unless ordering is guaranteed by your transport and topology.

Prefer designs that are resilient to:

- Out-of-order delivery
- Delayed delivery
- Redelivery after partial progress

When a workflow depends on order or prior state, model that explicitly in durable state (for example saga or persisted state checks), rather than in-memory assumptions.

## Keep contracts stable and version consciously

Message contracts evolve over time. To reduce breakage:

- Add fields in a backward-compatible way
- Avoid reinterpreting existing fields
- Keep old fields during migration windows
- Prefer additive changes over breaking renames/removals

For breaking changes, publish a new message type/version and transition producers/consumers gradually.

## Consider observability from day one

At minimum, include:

- Logs at meaningful state transitions
- Correlation/causation identifiers in log scopes where available
- Metrics around throughput, failures, and retry counts

Without observability, operational issues in background message processing are difficult to triage.

## Consumer checklist

Before shipping a consumer, confirm:

1. The handler is focused and delegates reusable business logic
2. Processing is idempotent for duplicate deliveries
3. Cancellation tokens flow through all async dependencies
4. Exceptions are not swallowed or hidden
5. Ordering assumptions are explicit and transport-aware
6. Logging and metrics provide enough context to debug failures

## Related docs

- [Getting Started](../tutorials/getting-started)
- [Architecture](../explanation/architecture)
