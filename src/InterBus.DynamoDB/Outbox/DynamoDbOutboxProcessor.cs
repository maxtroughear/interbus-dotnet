using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using InterBus.Sending;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace InterBus.DynamoDB.Outbox;

/// <summary>
/// Background service that continuously polls the DynamoDB outbox table for pending groups and
/// forwards their messages to the configured transport via <see cref="SendPipelineExecutor"/>.
/// <para>
/// One independent poll loop runs per processor shard (see
/// <see cref="DynamoDbOutboxOptions.ProcessorShardCount"/>). All loops execute in parallel,
/// allowing multiple host instances to share throughput with minimal coordination. Each loop
/// queries the GSI for up to <see cref="DynamoDbOutboxOptions.ProcessorQueryBatchSize"/>
/// candidates and tries each in turn, avoiding additional round trips when another instance
/// wins the lock on the first candidate.
/// </para>
/// <para>
/// Locking is implemented by pushing <c>VisibleAt</c> forward to
/// <c>now + ProcessorLockDuration</c> via a conditional <c>UpdateItem</c>
/// (<c>VisibleAt = :currentVat</c>). On failure the lock is released by resetting
/// <c>VisibleAt</c> to <c>now</c>.
/// </para>
/// <para>
/// The poll delay (<see cref="DynamoDbOutboxOptions.PollInterval"/>) is only applied when a
/// shard is empty. While messages are available the loop continues immediately.
/// </para>
/// <para>
/// Delivery is at-least-once: if the process crashes after forwarding but before deleting, the
/// group will be re-forwarded after the processor lock expires. Consumers should be idempotent.
/// </para>
/// </summary>
internal sealed partial class DynamoDbOutboxProcessor(
    IServiceScopeFactory scopeFactory,
    IAmazonDynamoDB dynamoDb,
    DynamoDbOutboxOptions options,
    ILogger<DynamoDbOutboxProcessor> logger)
    : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var processorShardCount = options.EffectiveProcessorShardCount;

        // For a single shard, run inline to avoid unnecessary Task allocation.
        if (processorShardCount <= 1)
            return RunShardLoopAsync(0, stoppingToken);

        // Spawn one independent loop per shard so they run in parallel.
        var shardTasks = Enumerable.Range(0, processorShardCount)
            .Select(shard => RunShardLoopAsync(shard, stoppingToken))
            .ToArray();

        return Task.WhenAll(shardTasks);
    }

    private async Task RunShardLoopAsync(int shard, CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var hadMessages = false;
            try
            {
                hadMessages = await ProcessShardAsync(shard, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex,
                    "InterBus DynamoDB outbox: unexpected error during group processing for shard {Shard}",
                    shard);
            }

            if (hadMessages) continue;

            try
            {
                await Task.Delay(options.PollInterval, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }
    }

    /// <summary>
    /// Processes one eligible group from <paramref name="shard"/>. Returns <c>true</c> if a
    /// group was found (and forwarded or failed); <c>false</c> when the shard is empty.
    /// </summary>
    private async Task<bool> ProcessShardAsync(int shard, CancellationToken cancellationToken)
    {
        var now = DateTimeOffset.UtcNow;
        var nowEpoch = now.ToUnixTimeSeconds().ToString();
        var lockExpiry = now.Add(options.ProcessorLockDuration).ToUnixTimeSeconds().ToString();

        // Fetch multiple candidates so that, when another processor wins the lock on the
        // first item, we can try the next candidate without a round trip to the GSI.
        var candidates = await QueryEligibleGroupsAsync(shard, nowEpoch, cancellationToken);

        foreach (var groupItem in candidates)
        {
            var groupId = groupItem[DynamoDbOutboxContext.AttrGroupId].S;
            var currentVisibleAt = groupItem[DynamoDbOutboxContext.AttrVisibleAt].N;

            // Acquire lock by pushing VisibleAt into the future. The condition ensures we
            // win a race against another processor that may have read the same group.
            try
            {
                await dynamoDb.UpdateItemAsync(new UpdateItemRequest
                {
                    TableName = options.TableName,
                    Key = BuildGroupKey(groupId),
                    UpdateExpression = "SET #vat = :lockExpiry",
                    ConditionExpression = "#vat = :currentVat",
                    ExpressionAttributeNames = new Dictionary<string, string>
                    {
                        ["#vat"] = DynamoDbOutboxContext.AttrVisibleAt,
                    },
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                    {
                        [":lockExpiry"] = new() { N = lockExpiry },
                        [":currentVat"] = new() { N = currentVisibleAt },
                    },
                }, cancellationToken);
            }
            catch (ConditionalCheckFailedException)
            {
                // Another processor grabbed this group — try the next candidate.
                continue;
            }

            await ForwardGroupAsync(groupId, nowEpoch, cancellationToken);
            return true;
        }

        return false;
    }

    /// <summary>
    /// Queries the GSI for up to <see cref="DynamoDbOutboxOptions.ProcessorQueryBatchSize"/>
    /// eligible group headers in the given shard.
    /// </summary>
    /// <remarks>
    /// GSI queries in DynamoDB always use eventually consistent reads; strongly consistent
    /// reads are not supported on GSIs. This is acceptable here because the conditional
    /// <c>UpdateItem</c> (lock acquisition) provides the authoritative consistency guarantee.
    /// </remarks>
    private async Task<List<Dictionary<string, AttributeValue>>> QueryEligibleGroupsAsync(
        int shard, string nowEpoch, CancellationToken cancellationToken)
    {
        var queryResponse = await dynamoDb.QueryAsync(new QueryRequest
        {
            TableName = options.TableName,
            IndexName = options.GsiIndexName,
            KeyConditionExpression = "#shard = :shard AND #vat <= :now",
            Limit = options.ProcessorQueryBatchSize,
            ExpressionAttributeNames = new Dictionary<string, string>
            {
                ["#shard"] = DynamoDbOutboxContext.AttrShard,
                ["#vat"] = DynamoDbOutboxContext.AttrVisibleAt,
            },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":shard"] = new() { N = shard.ToString() },
                [":now"] = new() { N = nowEpoch },
            },
        }, cancellationToken);

        return queryResponse.Items;
    }

    /// <summary>
    /// Fetches all message items for <paramref name="groupId"/>, forwards them to the
    /// transport, then deletes the group atomically.
    /// </summary>
    private async Task ForwardGroupAsync(string groupId, string nowEpoch,
        CancellationToken cancellationToken)
    {
        // Query all items for this group (header + messages), then exclude the header
        // client-side. DynamoDB does not allow primary key attributes in FilterExpression.
        var queryResponse = await dynamoDb.QueryAsync(new QueryRequest
        {
            TableName = options.TableName,
            KeyConditionExpression = "#gid = :gid",
            ExpressionAttributeNames = new Dictionary<string, string>
            {
                ["#gid"] = DynamoDbOutboxContext.AttrGroupId,
            },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":gid"] = new() { S = groupId },
            },
        }, cancellationToken);

        var messages = queryResponse.Items
            .Where(i => i[DynamoDbOutboxContext.AttrEntryId].S != DynamoDbOutboxContext.GroupEntryId)
            .ToList();

        LogForwardingGroup(groupId, messages.Count);

        await using var scope = scopeFactory.CreateAsyncScope();
        var pipeline = scope.ServiceProvider.GetRequiredService<SendPipelineExecutor>();

        foreach (var msg in messages.OrderBy(m => m[DynamoDbOutboxContext.AttrEntryId].S))
        {
            try
            {
                cancellationToken.ThrowIfCancellationRequested();

                var destination = new Uri(msg[DynamoDbOutboxContext.AttrDestination].S);
                var payload = msg[DynamoDbOutboxContext.AttrPayload].B.ToArray();
                var headers = DynamoDbOutboxContext.DeserializeHeaders(
                    msg[DynamoDbOutboxContext.AttrHeadersJson].S);
                TimeSpan? delay = null;
                if (msg.TryGetValue(DynamoDbOutboxContext.AttrDelay, out var delayAttr) &&
                    long.TryParse(delayAttr.N, out var delaySecs))
                    delay = TimeSpan.FromSeconds(delaySecs);

                var sendContext = new SendContext(destination, payload, headers, delay);
                await pipeline.ExecuteAsync(sendContext, cancellationToken: cancellationToken);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                var msgId = msg.TryGetValue(DynamoDbOutboxContext.AttrEntryId, out var eid)
                    ? eid.S
                    : "unknown";
                var dest = msg.TryGetValue(DynamoDbOutboxContext.AttrDestination, out var d)
                    ? d.S
                    : "unknown";
                logger.LogError(ex,
                    "InterBus DynamoDB outbox: failed to forward message {MessageId} in group {GroupId} to {Destination}",
                    msgId, groupId, dest);

                await ReleaseLockAsync(groupId, nowEpoch);
                return;
            }
        }

        // Delete group header and all message items atomically.
        var deleteItems = new List<TransactWriteItem>(messages.Count + 1)
        {
            new()
            {
                Delete = new Delete
                {
                    TableName = options.TableName,
                    Key = BuildGroupKey(groupId),
                },
            },
        };

        foreach (var msg in messages)
        {
            deleteItems.Add(new TransactWriteItem
            {
                Delete = new Delete
                {
                    TableName = options.TableName,
                    Key = new Dictionary<string, AttributeValue>
                    {
                        [DynamoDbOutboxContext.AttrGroupId] = new() { S = groupId },
                        [DynamoDbOutboxContext.AttrEntryId] =
                            new() { S = msg[DynamoDbOutboxContext.AttrEntryId].S },
                    },
                },
            });
        }

        try
        {
            await dynamoDb.TransactWriteItemsAsync(new TransactWriteItemsRequest
            {
                TransactItems = deleteItems,
            }, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogError(ex,
                "InterBus DynamoDB outbox: failed to delete group {GroupId} after forwarding",
                groupId);
            await ReleaseLockAsync(groupId, nowEpoch);
        }
    }

    /// <summary>
    /// Releases the lock on <paramref name="groupId"/> by resetting <c>VisibleAt</c> to
    /// <paramref name="nowEpoch"/>, making the group immediately eligible for the next
    /// processing attempt.
    /// </summary>
    private async Task ReleaseLockAsync(string groupId, string nowEpoch)
    {
        try
        {
            await dynamoDb.UpdateItemAsync(new UpdateItemRequest
            {
                TableName = options.TableName,
                Key = BuildGroupKey(groupId),
                UpdateExpression = "SET #vat = :now",
                ConditionExpression = "attribute_exists(#gid)",
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    ["#vat"] = DynamoDbOutboxContext.AttrVisibleAt,
                    ["#gid"] = DynamoDbOutboxContext.AttrGroupId,
                },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [":now"] = new() { N = nowEpoch },
                },
            });
        }
        catch (Exception ex) when (
            ex is ConditionalCheckFailedException or ResourceNotFoundException)
        {
            // Group already deleted — nothing to do.
        }
    }

    private static Dictionary<string, AttributeValue> BuildGroupKey(string groupId) =>
        new()
        {
            [DynamoDbOutboxContext.AttrGroupId] = new() { S = groupId },
            [DynamoDbOutboxContext.AttrEntryId] = new() { S = DynamoDbOutboxContext.GroupEntryId },
        };

    [LoggerMessage(LogLevel.Debug,
        "InterBus DynamoDB outbox: forwarding group {GroupId} ({Count} message(s))")]
    partial void LogForwardingGroup(string groupId, int count);
}
