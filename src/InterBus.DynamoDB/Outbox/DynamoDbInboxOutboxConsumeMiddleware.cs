using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using InterBus.Consuming;
using InterBus.Dispatching;
using InterBus.Sending;
using Microsoft.Extensions.Logging;

namespace InterBus.DynamoDB.Outbox;

internal enum DynamoDbInboxProcessingState
{
    PendingConsumer = 0,
    PendingOutbox = 1,
    Completed = 2,
}

/// <summary>
/// Consume middleware that provides stateful inbox+outbox handling backed by DynamoDB.
/// It ensures the consumer runs at most once per (consumer,messageId), and redeliveries
/// in <c>PendingOutbox</c> state skip the consumer and only retry outbox delivery.
/// </summary>
internal sealed partial class DynamoDbInboxOutboxConsumeMiddleware(
    IAmazonDynamoDB dynamoDb,
    DynamoDbOutboxOptions options,
    DynamoDbOutboxConsumeState consumeState,
    DynamoDbOutboxContext outboxContext,
    SendPipelineExecutor pipeline,
    ILogger<DynamoDbInboxOutboxConsumeMiddleware> logger)
    : IConsumeMiddleware
{
    private bool _loop;

    public async Task InvokeAsync<T>(ConsumeContext<T> context,
        Func<ConsumeContext<T>, CancellationToken, Task> next,
        CancellationToken cancellationToken = default) where T : class
    {
        if (!context.Headers.TryGetValue(InterBusHeaders.MessageId, out var messageId) ||
            string.IsNullOrWhiteSpace(messageId))
        {
            logger.LogWarning(
                "InterBus DynamoDB inbox+outbox: message received without {MessageId} header; skipping inbox",
                InterBusHeaders.MessageId);

            await next(context, cancellationToken);
            return;
        }

        var consumerName = context.Descriptor.ConsumerType.FullName ?? context.Descriptor.ConsumerType.Name;
        var inboxKey = BuildInboxKey(consumerName, messageId);

        consumeState.DeliveryType = DeliveryType.Inbox;

        try
        {
            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                _loop = false;

                var inboxItem = await GetInboxItemAsync(inboxKey, cancellationToken);
                if (inboxItem is null)
                {
                    await TryCreatePendingConsumerAsync(inboxKey, cancellationToken);
                    _loop = true;
                    continue;
                }

                var state = ReadState(inboxItem);

                var inboxTask = state switch
                {
                    DynamoDbInboxProcessingState.Completed => Task.CompletedTask,
                    DynamoDbInboxProcessingState.PendingConsumer =>
                        RunConsumer(inboxKey, context, next, cancellationToken),
                    DynamoDbInboxProcessingState.PendingOutbox =>
                        DeliverPendingOutboxAsync(inboxKey, inboxItem, cancellationToken),
                    _ => throw new InvalidOperationException(
                        $"Invalid inbox state {state} for message {messageId} in consumer {consumerName}")
                };

                await inboxTask;
            } while (_loop);
        }
        catch (ConditionalCheckFailedException ex)
        {
            context.Result = DispatchResult.Release(ex);
        }
    }

    private async Task<Dictionary<string, AttributeValue>?> GetInboxItemAsync(
        Dictionary<string, AttributeValue> inboxKey, CancellationToken cancellationToken)
    {
        var response = await dynamoDb.GetItemAsync(new GetItemRequest
        {
            TableName = options.InboxTableName,
            Key = inboxKey,
            ConsistentRead = true,
        }, cancellationToken);

        return response.IsItemSet ? response.Item : null;
    }

    private async Task TryCreatePendingConsumerAsync(
        Dictionary<string, AttributeValue> inboxKey,
        CancellationToken cancellationToken)
    {
        var createdAtEpoch = DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString();
        var item = new Dictionary<string, AttributeValue>(inboxKey)
        {
            [DynamoDbInboxTable.State] =
                new() { N = ((int)DynamoDbInboxProcessingState.PendingConsumer).ToString() },
            [DynamoDbInboxTable.CreatedAt] = new() { N = createdAtEpoch },
        };

        await dynamoDb.PutItemAsync(new PutItemRequest
        {
            TableName = options.InboxTableName,
            Item = item,
            ConditionExpression = "attribute_not_exists(#consumerName) AND attribute_not_exists(#messageId)",
            ExpressionAttributeNames = new Dictionary<string, string>
            {
                ["#consumerName"] = DynamoDbInboxTable.ConsumerName,
                ["#messageId"] = DynamoDbInboxTable.MessageId,
            },
        }, cancellationToken);
    }

    private static DynamoDbInboxProcessingState ReadState(Dictionary<string, AttributeValue> item)
    {
        if (!item.TryGetValue(DynamoDbInboxTable.State, out var stateAttr) ||
            !int.TryParse(stateAttr.N, out var stateInt) ||
            !Enum.IsDefined(typeof(DynamoDbInboxProcessingState), stateInt))
        {
            throw new InvalidOperationException("DynamoDB inbox row has invalid state value.");
        }

        return (DynamoDbInboxProcessingState)stateInt;
    }

    private async Task RunConsumer<T>(
        Dictionary<string, AttributeValue> inboxKey,
        ConsumeContext<T> context,
        Func<ConsumeContext<T>, CancellationToken, Task> next,
        CancellationToken cancellationToken)
    {
        await next(context, cancellationToken);

        var nowEpoch = DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString();

        var groupId = consumeState.GroupId?.ToString("N");

        if (groupId is null)
        {
            MarkInboxCompletedEarly(inboxKey, nowEpoch);

            await outboxContext.CommitAsync(cancellationToken);
            return;
        }

        MarkInboxConsumed(inboxKey, groupId, nowEpoch);

        await outboxContext.CommitAsync(cancellationToken);
        _loop = true;
    }

    private async Task DeliverPendingOutboxAsync(
        Dictionary<string, AttributeValue> inboxKey,
        Dictionary<string, AttributeValue> inboxItem,
        CancellationToken cancellationToken)
    {
        if (!inboxItem.TryGetValue(DynamoDbInboxTable.GroupId, out var groupIdAttr) ||
            string.IsNullOrWhiteSpace(groupIdAttr.S))
        {
            await MarkInboxCompletedAsync(inboxKey, cancellationToken);
            return;
        }

        var groupId = groupIdAttr.S;
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
            .OrderBy(i => i[DynamoDbOutboxContext.AttrEntryId].S)
            .ToList();

        LogInlineDelivery(groupId, messages.Count);

        foreach (var msg in messages)
        {
            var destination = new Uri(msg[DynamoDbOutboxContext.AttrDestination].S);
            var payload = msg[DynamoDbOutboxContext.AttrPayload].B.ToArray();
            var headers = DynamoDbOutboxContext.DeserializeHeaders(msg[DynamoDbOutboxContext.AttrHeadersJson].S);
            TimeSpan? delay = null;
            if (msg.TryGetValue(DynamoDbOutboxContext.AttrDelay, out var delayAttr) &&
                long.TryParse(delayAttr.N, out var delaySecs))
                delay = TimeSpan.FromSeconds(delaySecs);

            var sendContext = new SendContext(destination, payload, headers, delay);
            await pipeline.ExecuteAsync(sendContext, cancellationToken: cancellationToken);
        }

        var nowEpoch = DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString();
        var transactItems = new List<TransactWriteItem>(messages.Count + 2)
        {
            new()
            {
                Update = new Update
                {
                    TableName = options.InboxTableName,
                    Key = inboxKey,
                    ConditionExpression = "#state = :pendingOutbox",
                    UpdateExpression = "SET #state = :completed, #deliveredAt = :now REMOVE #groupId",
                    ExpressionAttributeNames = new Dictionary<string, string>
                    {
                        ["#state"] = DynamoDbInboxTable.State,
                        ["#groupId"] = DynamoDbInboxTable.GroupId,
                        ["#deliveredAt"] = DynamoDbInboxTable.DeliveredAt,
                    },
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                    {
                        [":pendingOutbox"] =
                            new() { N = ((int)DynamoDbInboxProcessingState.PendingOutbox).ToString() },
                        [":completed"] =
                            new() { N = ((int)DynamoDbInboxProcessingState.Completed).ToString() },
                        [":now"] = new() { N = nowEpoch },
                    },
                },
            },
            new()
            {
                Delete = new Delete
                {
                    TableName = options.TableName,
                    Key = new Dictionary<string, AttributeValue>
                    {
                        [DynamoDbOutboxContext.AttrGroupId] = new() { S = groupId },
                        [DynamoDbOutboxContext.AttrEntryId] =
                            new() { S = DynamoDbOutboxContext.GroupEntryId },
                    },
                },
            },
        };

        foreach (var msg in messages)
        {
            transactItems.Add(new TransactWriteItem
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

        await dynamoDb.TransactWriteItemsAsync(new TransactWriteItemsRequest
        {
            TransactItems = transactItems,
        }, cancellationToken);
    }

    private void MarkInboxConsumed(Dictionary<string, AttributeValue> inboxKey, string groupId, string nowEpoch)
    {
        outboxContext.AddTransactItem(new TransactWriteItem
        {
            Update = new Update
            {
                TableName = options.InboxTableName,
                Key = inboxKey,
                ConditionExpression = "#state = :pendingConsumer",
                UpdateExpression = "SET #state = :pendingOutbox, #groupId = :groupId, #consumedAt = :now",
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    ["#state"] = DynamoDbInboxTable.State,
                    ["#groupId"] = DynamoDbInboxTable.GroupId,
                    ["#consumedAt"] = DynamoDbInboxTable.ConsumedAt,
                },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [":pendingConsumer"] =
                        new() { N = ((int)DynamoDbInboxProcessingState.PendingConsumer).ToString() },
                    [":pendingOutbox"] =
                        new() { N = ((int)DynamoDbInboxProcessingState.PendingOutbox).ToString() },
                    [":groupId"] = new() { S = groupId },
                    [":now"] = new() { N = nowEpoch },
                },
            },
        });
    }

    private void MarkInboxCompletedEarly(Dictionary<string, AttributeValue> inboxKey, string nowEpoch)
    {
        outboxContext.AddTransactItem(new TransactWriteItem
        {
            Update = new Update
            {
                TableName = options.InboxTableName,
                Key = inboxKey,
                ConditionExpression = "#state = :pendingConsumer",
                UpdateExpression = "SET #state = :completed, #consumedAt = :now, #deliveredAt = :now",
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    ["#state"] = DynamoDbInboxTable.State,
                    ["#consumedAt"] = DynamoDbInboxTable.ConsumedAt,
                    ["#deliveredAt"] = DynamoDbInboxTable.DeliveredAt,
                },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [":pendingConsumer"] =
                        new() { N = ((int)DynamoDbInboxProcessingState.PendingConsumer).ToString() },
                    [":completed"] =
                        new() { N = ((int)DynamoDbInboxProcessingState.Completed).ToString() },
                    [":now"] = new() { N = nowEpoch },
                },
            },
        });
    }

    private async Task MarkInboxCompletedAsync(
        Dictionary<string, AttributeValue> inboxKey,
        CancellationToken cancellationToken)
    {
        var nowEpoch = DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString();
        await dynamoDb.UpdateItemAsync(new UpdateItemRequest
        {
            TableName = options.InboxTableName,
            Key = inboxKey,
            ConditionExpression = "#state = :pendingOutbox",
            UpdateExpression = "SET #state = :completed, #deliveredAt = :now REMOVE #groupId",
            ExpressionAttributeNames = new Dictionary<string, string>
            {
                ["#state"] = DynamoDbInboxTable.State,
                ["#groupId"] = DynamoDbInboxTable.GroupId,
                ["#deliveredAt"] = DynamoDbInboxTable.DeliveredAt,
            },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":pendingOutbox"] = new() { N = ((int)DynamoDbInboxProcessingState.PendingOutbox).ToString() },
                [":completed"] = new() { N = ((int)DynamoDbInboxProcessingState.Completed).ToString() },
                [":now"] = new() { N = nowEpoch },
            },
        }, cancellationToken);
    }

    private static Dictionary<string, AttributeValue> BuildInboxKey(string consumerName, string messageId) =>
        new()
        {
            [DynamoDbInboxTable.ConsumerName] = new() { S = consumerName },
            [DynamoDbInboxTable.MessageId] = new() { S = messageId },
        };

    [LoggerMessage(LogLevel.Debug,
        "InterBus DynamoDB inbox+outbox: delivery for group {GroupId} ({Count} message(s))")]
    partial void LogInlineDelivery(string groupId, int count);
}
