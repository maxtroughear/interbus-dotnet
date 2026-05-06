using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using InterBus.Consuming;
using InterBus.Sending;
using Microsoft.Extensions.Logging;

namespace InterBus.DynamoDB.Outbox.Internal;

/// <summary>
/// Consume middleware that attempts immediate ("inline") delivery of outbox messages written by
/// <see cref="DynamoDbOutboxPublishMiddleware"/> during the same consumer dispatch, after
/// <see cref="IDynamoDbOutboxContext.CommitAsync"/> has been called by the consumer handler.
/// <para>
/// After the consumer's <c>Consume</c> handler returns (including any
/// <see cref="IDynamoDbOutboxContext.CommitAsync"/> call that persists the outbox group),
/// this middleware reads the committed group from DynamoDB and forwards each message directly to
/// the transport via <see cref="SendPipelineExecutor"/>. If every message is delivered
/// successfully the group is deleted atomically, saving a background-processor round-trip.
/// </para>
/// <para>
/// If delivery fails for any reason the exception is swallowed and the group remains in DynamoDB
/// with its <c>VisibleAt</c> timestamp intact, so the background processor will retry it after
/// <see cref="DynamoDbOutboxOptions.ConsumerVisibilityTimeout"/> has elapsed.
/// </para>
/// </summary>
internal sealed partial class DynamoDbOutboxInlineDeliveryConsumeMiddleware(
    IAmazonDynamoDB dynamoDb,
    DynamoDbOutboxOptions options,
    DynamoDbOutboxConsumeState consumeState,
    SendPipelineExecutor pipeline,
    ILogger<DynamoDbOutboxInlineDeliveryConsumeMiddleware> logger)
    : IConsumeMiddleware
{
    public async Task InvokeAsync<T>(ConsumeContext<T> context,
        Func<ConsumeContext<T>, CancellationToken, Task> next,
        CancellationToken cancellationToken = default) where T : class
    {
        consumeState.IsDeliveryInline = true;

        // Run the consumer handler. The consumer is expected to call CommitAsync inside
        // Consume, which atomically persists any outbox entries to DynamoDB.
        await next(context, cancellationToken);

        if (consumeState.GroupId is not { } groupId)
            return;

        var groupIdStr = groupId.ToString("N");

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
                [":gid"] = new() { S = groupIdStr },
            },
        }, cancellationToken);

        var messages = queryResponse.Items
            .Where(i => i[DynamoDbOutboxContext.AttrEntryId].S != DynamoDbOutboxContext.GroupEntryId)
            .ToList();

        if (messages.Count == 0)
            return;

        LogInlineDelivery(groupIdStr, messages.Count);

        try
        {
            foreach (var msg in messages.OrderBy(m => m[DynamoDbOutboxContext.AttrEntryId].S))
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
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            // Inline delivery failed — leave the group for the background processor.
            LogInlineDeliveryFailed(groupIdStr, ex);
            return;
        }

        // Delete group header and all message items atomically.
        var deleteItems = new List<TransactWriteItem>(messages.Count + 1)
        {
            new()
            {
                Delete = new Delete
                {
                    TableName = options.TableName,
                    Key = new Dictionary<string, AttributeValue>
                    {
                        [DynamoDbOutboxContext.AttrGroupId] = new() { S = groupIdStr },
                        [DynamoDbOutboxContext.AttrEntryId] =
                            new() { S = DynamoDbOutboxContext.GroupEntryId },
                    },
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
                        [DynamoDbOutboxContext.AttrGroupId] = new() { S = groupIdStr },
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
        catch (Exception ex)
        {
            // Delete failed — the processor will pick up and re-deliver. Consumers should
            // be idempotent.
            LogInlineDeleteFailed(groupIdStr, ex);
        }
    }

    [LoggerMessage(LogLevel.Debug,
        "InterBus DynamoDB outbox: inline delivery for group {GroupId} ({Count} message(s))")]
    partial void LogInlineDelivery(string groupId, int count);

    [LoggerMessage(LogLevel.Warning,
        "InterBus DynamoDB outbox: inline delivery failed for group {GroupId}; left for background processor")]
    partial void LogInlineDeliveryFailed(string groupId, Exception ex);

    [LoggerMessage(LogLevel.Warning,
        "InterBus DynamoDB outbox: failed to delete inline-delivered group {GroupId}; processor will re-deliver")]
    partial void LogInlineDeleteFailed(string groupId, Exception ex);
}
