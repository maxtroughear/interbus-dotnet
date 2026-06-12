---
sidebar_position: 3
---

# EF Core outbox and inbox

This guide shows how to configure the EF Core outbox and inbox integrations for reliable publish and consumer-side idempotency.

## What you get

- **Outbox**: writes outgoing messages to your `DbContext` transaction, then forwards them asynchronously
- **Inbox**: deduplicates consumer processing by message ID and persists consumer-published messages safely
- **FIFO partitioning**: outbox groups are keyed by `interbus-message-group-id`, with ordering guaranteed at least per group key

## 1. Add EF Core entities to your DbContext model

Call `AddInterBusOutbox()` in `OnModelCreating`.

```csharp
public sealed class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.AddInterBusOutbox();
    }
}
```

## 2. Register InterBus with EF Core outbox

Use `AddEfCore<TDbContext>()` plus `UseEfCoreOutbox<TDbContext>()`.

> InterBus does **not** register your `DbContext` for you.  
> Register it normally (for example `services.AddDbContext<AppDbContext>(...)`) before configuring InterBus EF Core integration.

```csharp
services.AddDbContext<AppDbContext>(options =>
{
    options.UseNpgsql(connectionString); // or UseSqlServer(...)
});

services.AddInterBus(bus =>
{
    bus.AddInMemoryTransport(); // or your transport
    bus.AddEfCore<AppDbContext>();

    bus.UseEfCoreOutbox<AppDbContext>(outbox => outbox
        .UsePostgreSql() // or UseSqlServer()
        .WithShardCount(128)
        .WithMaxConcurrentWorkers(8)
        .WithEmptyPollDelay(TimeSpan.FromMilliseconds(50)));
});
```

## 3. Create and apply migrations

The EF outbox/inbox tables are part of your model once `AddInterBusOutbox()` is added.

```bash
dotnet ef migrations add AddInterBusOutbox
dotnet ef database update
```

## Consumer-side inbox + outbox

To make a consumer idempotent and safely publish follow-up messages in the same durable flow:

```csharp
services.AddInterBus(bus =>
{
    bus.AddEfCore<AppDbContext>();
    bus.AddConsumer<OrderPlacedConsumer>(c =>
        c.UseEfCoreInboxOutbox<AppDbContext>());
});
```

This enables:

- inbox dedupe by `(consumer, message-id)`
- safe persistence of consumer-published outbox messages
- retry-friendly behavior if downstream delivery fails

## Ordering model

- Outbox publish middleware partitions buffered groups by `InterBusHeaders.MessageGroupId`
- Shard assignment is deterministic from `message-group-id`
- Processing is FIFO within a shard, therefore at least FIFO for a given message group ID
- There is no global FIFO guarantee when multiple shards/workers are used

## Lock providers and multi-instance processing

For multi-instance deployments, use a DB lock provider:

- PostgreSQL: `UsePostgreSql()` (`FOR UPDATE SKIP LOCKED`)
- SQL Server: `UseSqlServer()` (`UPDLOCK, READPAST`)

The default EF lock provider is for single-instance/test use only.

## Key options

| Option | Meaning |
|---|---|
| `WithShardCount(int)` | Number of FIFO shards (**default: 128**). Higher values increase parallelism and reduce global ordering scope |
| `WithMaxConcurrentWorkers(int)` | Number of outbox workers in this process |
| `WithEmptyPollDelay(TimeSpan)` | Backoff delay when no claimable outbox work is found |
| `DisableOutboxProcessor()` | Disable background forwarding (for specialized scenarios/tests) |

## Operational notes

- Outbox delivery is **at-least-once**; consumers should remain idempotent
- Keep `MessageGroupId` stable for workflows that require in-order processing
- Use database-native lock providers for production throughput and safe multi-processor behavior

:::warning Changing `ShardCount` in production
Changing shard count changes hash partitioning immediately. That means the same `MessageGroupId` can map to a different shard after deployment, which can change throughput/fairness characteristics during rollout and may affect ordering expectations if old/new versions are mixed.

Treat shard-count changes as an operational migration:

1. Prefer a coordinated rollout (avoid long mixed-version windows)
2. Consider draining or reducing outbox backlog before the change
3. Monitor throughput, lag, and consumer behavior closely after rollout
:::

## Related docs

- [Writing consumers](./writing-consumers)
- [InterBus concepts](../explanation/concepts)
