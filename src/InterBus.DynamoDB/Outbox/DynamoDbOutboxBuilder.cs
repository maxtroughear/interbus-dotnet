using Microsoft.Extensions.DependencyInjection;

namespace InterBus.DynamoDB.Outbox;

/// <summary>
/// Fluent builder for configuring the InterBus DynamoDB transactional outbox.
/// Obtained via <c>bus.UseDynamoDbOutbox(outbox =&gt; { ... })</c> or
/// <c>bus.AddDynamoDbOutbox(outbox =&gt; { ... })</c>.
/// </summary>
public sealed class DynamoDbOutboxBuilder
{
    /// <summary>The underlying service collection.</summary>
    public IServiceCollection Services { get; }

    internal DynamoDbOutboxOptions Options { get; } = new();

    internal DynamoDbOutboxBuilder(IServiceCollection services)
    {
        Services = services;
    }

    /// <summary>
    /// Overrides how often the background processor polls the outbox table when there are no
    /// pending messages. Defaults to 5 seconds.
    /// </summary>
    public DynamoDbOutboxBuilder WithPollInterval(TimeSpan interval)
    {
        Options.PollInterval = interval;
        return this;
    }

    /// <summary>
    /// Sets the visibility timeout for outbox groups created by consumer publish calls.
    /// When a consumer publishes a message during its consume handler, the outbox group is
    /// created with a <c>VisibleAt</c> timestamp of <c>UtcNow + timeout</c>, preventing the
    /// processor from racing with inline delivery. Defaults to 5 seconds.
    /// </summary>
    public DynamoDbOutboxBuilder WithConsumerVisibilityTimeout(TimeSpan timeout)
    {
        Options.ConsumerVisibilityTimeout = timeout;
        return this;
    }

    /// <summary>
    /// Sets the DynamoDB table name used for outbox storage. Defaults to
    /// <c>"InterBusOutbox"</c>.
    /// </summary>
    public DynamoDbOutboxBuilder WithTableName(string tableName)
    {
        Options.TableName = tableName;
        return this;
    }

    /// <summary>
    /// Sets the number of shards for distributing outbox groups. Higher values allow more
    /// processor instances to work in parallel (one per shard) without contention.
    /// Requires a GSI — see <see cref="WithGsiIndexName"/>. Defaults to 1.
    /// </summary>
    public DynamoDbOutboxBuilder WithShardCount(int shardCount)
    {
        Options.ShardCount = shardCount;
        return this;
    }

    /// <summary>
    /// Sets the name of the GSI used to query eligible groups by shard and <c>VisibleAt</c>.
    /// The index must have <c>Shard</c> (Number) as the partition key and <c>VisibleAt</c>
    /// (Number) as the sort key. Set to <c>null</c> to fall back to a full-table scan.
    /// Defaults to <c>"InterBusOutbox-VisibleAt-index"</c>.
    /// </summary>
    public DynamoDbOutboxBuilder WithGsiIndexName(string? indexName)
    {
        Options.GsiIndexName = indexName;
        return this;
    }

    /// <summary>
    /// Disables the background <see cref="Internal.DynamoDbOutboxProcessor"/> hosted service.
    /// Useful when the processor is run in a separate process or when tests need direct
    /// control over message forwarding.
    /// </summary>
    public DynamoDbOutboxBuilder DisableOutboxProcessor()
    {
        Options.RunOutboxProcessor = false;
        return this;
    }
}
