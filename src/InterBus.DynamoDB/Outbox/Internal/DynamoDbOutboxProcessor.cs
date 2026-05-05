using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using InterBus.Sending;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace InterBus.DynamoDB.Outbox.Internal;

/// <summary>
/// Background service that continuously polls the DynamoDB outbox table for pending groups and
/// forwards their messages to the configured transport via <see cref="SendPipelineExecutor"/>.
/// <para>
/// Each iteration scans for one eligible group (where <c>VisibleAt</c> has elapsed and no
/// active processor lock is held), acquires an optimistic lock using a conditional
/// <c>UpdateItem</c>, forwards all messages in the group, then deletes the group and messages
/// atomically via <c>TransactWriteItems</c>.
/// </para>
/// <para>
/// The poll delay (<see cref="DynamoDbOutboxOptions.PollInterval"/>) is only applied when the
/// outbox is empty. While messages are available the processor loops immediately.
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
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            bool hadMessages;
            try
            {
                hadMessages = await ProcessGroupAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "InterBus DynamoDB outbox: unexpected error during group processing");
                hadMessages = false;
            }

            if (!hadMessages)
            {
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
    }

    /// <summary>
    /// Processes one group. Returns <c>true</c> if a group was found (and forwarded or
    /// failed); <c>false</c> when the outbox is empty.
    /// </summary>
    private async Task<bool> ProcessGroupAsync(CancellationToken cancellationToken)
    {
        var now = DateTimeOffset.UtcNow;
        var nowEpoch = now.ToUnixTimeSeconds().ToString();

        // Scan for GROUP header items that are visible and not locked by another processor.
        // The outbox table is expected to be small (messages are deleted after delivery)
        // so a scan is acceptable. A GSI on VisibleAt can be added for higher-throughput
        // scenarios if needed.
        var scanResponse = await dynamoDb.ScanAsync(new ScanRequest
        {
            TableName = options.TableName,
            FilterExpression =
                "#eid = :group AND #vat <= :now AND " +
                "(attribute_not_exists(#pid) OR #ple < :now)",
            ExpressionAttributeNames = new Dictionary<string, string>
            {
                ["#eid"] = DynamoDbOutboxContext.AttrEntryId,
                ["#vat"] = DynamoDbOutboxContext.AttrVisibleAt,
                ["#pid"] = DynamoDbOutboxContext.AttrProcessorId,
                ["#ple"] = DynamoDbOutboxContext.AttrProcessorLockExpiresAt,
            },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":group"] = new() { S = DynamoDbOutboxContext.GroupEntryId },
                [":now"] = new() { N = nowEpoch },
            },
        }, cancellationToken);

        if (scanResponse.Count == 0)
            return false;

        var groupItem = scanResponse.Items[0];
        var groupId = groupItem[DynamoDbOutboxContext.AttrGroupId].S;

        // Try to acquire the processor lock via a conditional update.
        var processorId = Guid.NewGuid().ToString("N");
        var lockExpiresAt = now.Add(options.ProcessorLockDuration).ToUnixTimeSeconds().ToString();

        try
        {
            await dynamoDb.UpdateItemAsync(new UpdateItemRequest
            {
                TableName = options.TableName,
                Key = BuildGroupKey(groupId),
                UpdateExpression = "SET #pid = :pid, #ple = :ple",
                ConditionExpression = "attribute_not_exists(#pid) OR #ple < :now",
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    ["#pid"] = DynamoDbOutboxContext.AttrProcessorId,
                    ["#ple"] = DynamoDbOutboxContext.AttrProcessorLockExpiresAt,
                },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [":pid"] = new() { S = processorId },
                    [":ple"] = new() { N = lockExpiresAt },
                    [":now"] = new() { N = nowEpoch },
                },
            }, cancellationToken);
        }
        catch (ConditionalCheckFailedException)
        {
            // Another processor grabbed this group — return true so the caller retries
            // immediately without waiting the poll interval.
            return true;
        }

        // Query all message items for this group (i.e. SK != "GROUP").
        var queryResponse = await dynamoDb.QueryAsync(new QueryRequest
        {
            TableName = options.TableName,
            KeyConditionExpression = "#gid = :gid",
            FilterExpression = "#eid <> :group",
            ExpressionAttributeNames = new Dictionary<string, string>
            {
                ["#gid"] = DynamoDbOutboxContext.AttrGroupId,
                ["#eid"] = DynamoDbOutboxContext.AttrEntryId,
            },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":gid"] = new() { S = groupId },
                [":group"] = new() { S = DynamoDbOutboxContext.GroupEntryId },
            },
        }, cancellationToken);

        var messages = queryResponse.Items;

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
                    "InterBus DynamoDB outbox: failed to forward message {MessageId} to {Destination}",
                    msgId, dest);

                await ReleaseLockAsync(groupId, processorId);
                return true; // group existed; retry next iteration
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
            await ReleaseLockAsync(groupId, processorId);
        }

        return true;
    }

    private async Task ReleaseLockAsync(string groupId, string processorId)
    {
        try
        {
            await dynamoDb.UpdateItemAsync(new UpdateItemRequest
            {
                TableName = options.TableName,
                Key = BuildGroupKey(groupId),
                UpdateExpression = "REMOVE #pid, #ple",
                ConditionExpression = "#pid = :pid",
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    ["#pid"] = DynamoDbOutboxContext.AttrProcessorId,
                    ["#ple"] = DynamoDbOutboxContext.AttrProcessorLockExpiresAt,
                },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [":pid"] = new() { S = processorId },
                },
            });
        }
        catch (Exception ex) when (
            ex is ConditionalCheckFailedException or ResourceNotFoundException)
        {
            // Lock already released or group deleted — nothing to do.
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
