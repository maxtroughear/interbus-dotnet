---
sidebar_position: 4
---

# DynamoDB outbox

This guide shows how to configure the `InterBus.DynamoDB` transactional outbox and how to run it for either stronger FIFO behavior or higher throughput.

## What you get

- **Transactional publish**: `IPublisher` calls are captured into DynamoDB and committed with your own `TransactWriteItem` operations
- **Background forwarding**: a hosted outbox processor forwards committed groups to your configured transport
- **At-least-once delivery**: if forwarding succeeds but deletion fails, messages can be forwarded again after lock expiry
- **FIFO by group**: ordering is maintained within each outbox group/message-group key, not globally across all groups

## 1. Register DynamoDB outbox

Use `UseDynamoDbOutbox(...)` when you want bus-level publish calls routed through the DynamoDB outbox.

```csharp
services.AddInterBus(bus =>
{
    bus.AddInMemoryTransport(); // or your transport
    bus.UseDynamoDbOutbox(options =>
    {
        options.TableName = "InterBusOutbox";
        options.InboxTableName = "InterBusInbox";
        options.ShardCount = 64;
        options.ProcessorShardCount = 64;
        options.PollInterval = TimeSpan.FromSeconds(1);
        options.ProcessorLockDuration = TimeSpan.FromSeconds(30);
        options.RunOutboxProcessor = true;
    });
});
```

## 2. Commit outbox writes

When outbox publish middleware is enabled, published messages are buffered until `CommitAsync` is called on `IDynamoDbOutboxContext`.

```csharp
public sealed class CheckoutService(IDynamoDbOutboxContext outbox, IPublisher publisher)
{
    public async Task PlaceOrderAsync(OrderPlaced message, CancellationToken cancellationToken)
    {
        await publisher.Publish(message, cancellationToken);

        // Add your own DynamoDB transact items here if needed
        // outbox.AddTransactItem(...);

        await outbox.CommitAsync(cancellationToken);
    }
}
```

## 3. Processor model (bus-scoped, shard-parallel)

- Enabling `RunOutboxProcessor` registers a hosted processor for this bus setup
- The processor runs **one independent loop task per processor shard**
- `ProcessorShardCount` controls how many shard loops run in this process (defaults to `ShardCount`)
- Shard loops run in parallel and each shard loop claims eligible group headers with conditional updates

## Ordering and throughput trade-offs

- Running the outbox processor on **one instance** can give more predictable FIFO behavior (still scoped to group/shard), but may reduce overall throughput
- If strict ordering is not required, running the processor on **multiple instances** usually increases throughput by processing more shards/groups concurrently
- There is no global FIFO guarantee across different message groups

## Key options

| Option | Meaning |
|---|---|
| `ShardCount` | Number of outbox shards used when writing group headers |
| `ProcessorShardCount` | Number of shards this process actively polls (defaults to `ShardCount`) |
| `ProcessorQueryBatchSize` | Candidate groups fetched per shard poll to reduce lock-contention round trips |
| `PollInterval` | Delay used when a shard has no work |
| `ProcessorLockDuration` | How long a claimed group stays locked before another processor can reclaim it |
| `RunOutboxProcessor` | Whether to run the hosted background outbox processor in this app instance |
| `CreateTablesOnStartup` | Dev/test convenience to create inbox/outbox tables automatically |

## Operational notes

- Keep consumers idempotent because delivery is at-least-once
- Keep `MessageGroupId` stable for workflows that need ordered handling
- For production, prefer managing DynamoDB table/index lifecycle with IaC instead of `CreateTablesOnStartup`

## Related docs

- [EF Core outbox and inbox](./efcore-outbox-inbox)
- [Writing consumers](./writing-consumers)
