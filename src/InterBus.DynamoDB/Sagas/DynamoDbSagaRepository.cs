using System.Text.Json;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using InterBus.Sagas;

namespace InterBus.DynamoDB.Sagas;

/// <summary>
/// DynamoDB-backed implementation of <see cref="ISagaRepository{TSaga, TState}"/>.
/// </summary>
/// <remarks>
/// <para>
/// Each saga instance is stored as a single DynamoDB item using a composite key:
/// <c>CorrelationId</c> (partition key) and <c>SagaType</c> (sort key). This allows
/// multiple saga types to share the same DynamoDB table without conflicts.
/// The following attributes are used:
/// </para>
/// <list type="table">
///   <item><term>CorrelationId</term><description>Partition key (string)</description></item>
///   <item><term>SagaType</term><description>Sort key — fully-qualified name of <typeparamref name="TSaga"/> (string)</description></item>
///   <item><term>State</term><description>JSON-serialised saga state (string)</description></item>
///   <item><term>IsCompleted</term><description>Completion flag (bool)</description></item>
///   <item><term>LockOwner</term><description>UUID of the current lock holder (string)</description></item>
///   <item><term>LockExpiresAt</term><description>Unix epoch seconds at which the current lock expires (number)</description></item>
///   <item><term>Ttl</term><description>Unix epoch seconds for DynamoDB TTL (number); requires TTL enabled on the table</description></item>
/// </list>
/// <para>
/// Exclusive locking uses a lease-style mechanism: <see cref="AcquireLockAsync"/> attempts a
/// conditional <c>UpdateItem</c> that succeeds only when no unexpired lock is held by another
/// caller. The lock owner GUID is stored alongside its expiry so that stale locks (e.g. from
/// crashed processes) are automatically reclaimed after <see cref="DynamoDbSagaOptions{TSaga}.LockLeaseDuration"/>.
/// </para>
/// </remarks>
/// <typeparam name="TSaga">The saga type.</typeparam>
/// <typeparam name="TState">The saga state type.</typeparam>
public sealed class DynamoDbSagaRepository<TSaga, TState>(
    IAmazonDynamoDB dynamoDb,
    DynamoDbSagaOptions<TSaga> options,
    IDynamoDbStateWriter<TSaga> stateWriter)
    : ISagaRepository<TSaga, TState>
    where TSaga : class, ISaga<TState>
    where TState : class, new()
{
    #region Table Attributes

    /// <summary>The partition key attribute name (hash key). String type.</summary>
    private const string AttrCorrelationId = "CorrelationId";

    /// <summary>
    /// The sort key attribute name. Stores the fully-qualified name of the saga type,
    /// allowing multiple saga types to share the same DynamoDB table. String type.
    /// </summary>
    private const string AttrSagaType = "SagaType";

    /// <summary>JSON-serialised saga state. String type.</summary>
    private const string AttrState = "State";

    /// <summary>Completion flag. Boolean type.</summary>
    private const string AttrIsCompleted = "IsCompleted";

    /// <summary>UUID of the current lock holder. String type.</summary>
    private const string AttrLockOwner = "LockOwner";

    /// <summary>Unix epoch seconds at which the current lock expires. Number type.</summary>
    private const string AttrLockExpiresAt = "LockExpiresAt";

    /// <summary>
    /// Unix epoch seconds used as the DynamoDB TTL attribute.
    /// Requires TTL to be enabled on the table pointing at this attribute name.
    /// Number type.
    /// </summary>
    private const string AttrTtl = "Ttl";

    #endregion

    private static readonly string SagaTypeName =
        typeof(TSaga).FullName ?? typeof(TSaga).Name;

    private readonly string _lockOwner = Guid.NewGuid().ToString("N");

    /// <inheritdoc/>
    /// <remarks>
    /// Implements a lease-style distributed lock using DynamoDB conditional expressions.
    /// Retries with a fixed delay until the lock is acquired or
    /// <see cref="DynamoDbSagaOptions{TSaga}.LockAcquireTimeout"/> is exceeded.
    /// </remarks>
    public async Task<IAsyncDisposable> AcquireLockAsync(
        string correlationId,
        CancellationToken cancellationToken = default)
    {
        var deadline = DateTimeOffset.UtcNow.Add(options.LockAcquireTimeout);

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (await TryAcquireLockAsync(correlationId, _lockOwner, cancellationToken))
                return new LockReleaser(this, correlationId, _lockOwner);

            if (DateTimeOffset.UtcNow >= deadline)
                throw new TimeoutException(
                    $"Failed to acquire saga lock for correlation ID '{correlationId}' " +
                    $"within {options.LockAcquireTimeout.TotalSeconds:F0} seconds.");

            await Task.Delay(options.LockRetryInterval, cancellationToken);
        }
    }

    /// <inheritdoc/>
    public async Task<SagaEntry<TState>?> GetAsync(
        string correlationId,
        CancellationToken cancellationToken = default)
    {
        var response = await dynamoDb.GetItemAsync(new GetItemRequest
        {
            TableName = options.TableName,
            Key = BuildKey(correlationId),
            ConsistentRead = true,
        }, cancellationToken);

        if (!response.IsItemSet)
            return null;

        var item = response.Item;

        // Resolve TTL: treat expired items as non-existent (lazy expiry)
        DateTimeOffset? expiresAtUtc = null;
        if (item.TryGetValue(AttrTtl, out var ttlAttr) &&
            long.TryParse(ttlAttr.N, out var ttlEpoch))
        {
            expiresAtUtc = DateTimeOffset.FromUnixTimeSeconds(ttlEpoch);
            if (expiresAtUtc.Value <= DateTimeOffset.UtcNow)
                return null;
        }

        var stateJson = item.TryGetValue(AttrState, out var stateAttr) ? stateAttr.S : null;
        if (stateJson is null)
            return null;

        var state = JsonSerializer.Deserialize<TState>(stateJson) ?? new TState();

        var entry = new SagaEntry<TState>(state, expiresAtUtc);

        if (item.TryGetValue(AttrIsCompleted, out var completedAttr) && completedAttr.BOOL is true)
            entry.IsCompleted = true;

        return entry;
    }

    /// <inheritdoc/>
    public async Task SaveAsync(
        string correlationId,
        SagaEntry<TState> entry,
        CancellationToken cancellationToken = default)
    {
        var stateJson = JsonSerializer.Serialize(entry.State);

        var item = new Dictionary<string, AttributeValue>
        {
            [AttrCorrelationId] = new() { S = correlationId },
            [AttrSagaType] = new() { S = SagaTypeName },
            [AttrState] = new() { S = stateJson },
            [AttrIsCompleted] = new() { BOOL = entry.IsCompleted },
            [AttrLockOwner] = new() { S = _lockOwner },
            [AttrLockExpiresAt] = new()
                { N = DateTimeOffset.UtcNow.Add(options.LockLeaseDuration).ToUnixTimeSeconds().ToString() },
        };

        if (entry.ExpiresAtUtc.HasValue)
        {
            item[AttrTtl] = new AttributeValue { N = entry.ExpiresAtUtc.Value.ToUnixTimeSeconds().ToString() };
        }

        await stateWriter.PutItemAsync(new PutItemRequest
        {
            TableName = options.TableName,
            Item = item,
            ConditionExpression = "#lo = :owner",
            ExpressionAttributeNames = new Dictionary<string, string>
            {
                ["#lo"] = AttrLockOwner
            },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":owner"] = new() { S = _lockOwner }
            }
        }, cancellationToken);
    }

    /// <inheritdoc/>
    public async Task DeleteAsync(
        string correlationId,
        CancellationToken cancellationToken = default)
    {
        await dynamoDb.DeleteItemAsync(new DeleteItemRequest
        {
            TableName = options.TableName,
            Key = BuildKey(correlationId),
        }, cancellationToken);
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// <summary>
    /// Attempts a single conditional write to claim the lock.
    /// Returns <c>true</c> when the lock was successfully acquired.
    /// </summary>
    private async Task<bool> TryAcquireLockAsync(
        string correlationId,
        string lockOwner,
        CancellationToken cancellationToken)
    {
        var now = DateTimeOffset.UtcNow;
        var lockExpiresAt = now.Add(options.LockLeaseDuration);
        var nowEpoch = now.ToUnixTimeSeconds().ToString();
        var lockExpiresAtEpoch = lockExpiresAt.ToUnixTimeSeconds().ToString();

        try
        {
            await dynamoDb.UpdateItemAsync(new UpdateItemRequest
            {
                TableName = options.TableName,
                Key = BuildKey(correlationId),
                UpdateExpression = "SET #lo = :owner, #le = :expiresAt",
                // Allow the lock to be taken when:
                //   1. No item exists yet (attribute_not_exists)
                //   2. The current lock has expired (:now > LockExpiresAt)
                ConditionExpression =
                    "attribute_not_exists(#lo) OR #le < :now",
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    ["#lo"] = AttrLockOwner,
                    ["#le"] = AttrLockExpiresAt,
                },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [":owner"] = new() { S = lockOwner },
                    [":expiresAt"] = new() { N = lockExpiresAtEpoch },
                    [":now"] = new() { N = nowEpoch },
                },
            }, cancellationToken);

            return true;
        }
        catch (ConditionalCheckFailedException)
        {
            return false;
        }
    }

    /// <summary>
    /// Releases the lock conditionally: only removes <c>LockOwner</c> / <c>LockExpiresAt</c>
    /// if the caller still owns the lock, preventing accidental release of a lock held by
    /// another caller after a lease expiry.
    /// </summary>
    private async Task ReleaseLockAsync(string correlationId, string lockOwner)
    {
        try
        {
            await dynamoDb.UpdateItemAsync(new UpdateItemRequest
            {
                TableName = options.TableName,
                Key = BuildKey(correlationId),
                UpdateExpression = "REMOVE #lo, #le",
                ConditionExpression = "#lo = :owner",
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    ["#lo"] = AttrLockOwner,
                    ["#le"] = AttrLockExpiresAt,
                },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [":owner"] = new() { S = lockOwner },
                },
            });
        }
        catch (ConditionalCheckFailedException)
        {
            // Lock was already reclaimed by another caller — nothing to do.
        }
        catch (ResourceNotFoundException)
        {
            // Item was deleted while the lock was held — nothing to do.
        }
    }

    private static Dictionary<string, AttributeValue> BuildKey(string correlationId) =>
        new()
        {
            [AttrCorrelationId] = new AttributeValue { S = correlationId },
            [AttrSagaType] = new AttributeValue { S = SagaTypeName },
        };

    // -----------------------------------------------------------------------
    // Lock releaser
    // -----------------------------------------------------------------------

    private sealed class LockReleaser(
        DynamoDbSagaRepository<TSaga, TState> repository,
        string correlationId,
        string lockOwner) : IAsyncDisposable
    {
        public ValueTask DisposeAsync() =>
            new(repository.ReleaseLockAsync(correlationId, lockOwner));
    }
}
