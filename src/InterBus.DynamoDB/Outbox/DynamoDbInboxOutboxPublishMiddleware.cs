using Amazon.DynamoDBv2.Model;
using InterBus.DeadLettering;
using InterBus.Publishing;

namespace InterBus.DynamoDB.Outbox;

internal class DynamoDbInboxOutboxPublishMiddleware(
    DynamoDbOutboxContext outboxContext,
    DynamoDbOutboxConsumeState consumeState,
    DynamoDbOutboxOptions options,
    IMessageSerializer serializer)
    : IPublishMiddleware
{
    private readonly string _createdAtEpoch = DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString();
    private string? _groupId;

    public Task InvokeAsync<T>(PublishContext<T> context, Func<PublishContext<T>, CancellationToken, Task> next,
        CancellationToken cancellationToken = default) where T : class
    {
        if (typeof(T).IsGenericType && typeof(T).GetGenericTypeDefinition() == typeof(DeadLetter<>))
            return next(context, cancellationToken);

        if (consumeState.GroupId is null)
        {
            consumeState.GroupId = Guid.CreateVersion7();
            _groupId = consumeState.GroupId.ToString();

            outboxContext.AddTransactItem(new TransactWriteItem
            {
                Put = new Put
                {
                    TableName = options.TableName,
                    Item = new Dictionary<string, AttributeValue>
                    {
                        [DynamoDbOutboxContext.AttrGroupId] = new() { S = _groupId },
                        [DynamoDbOutboxContext.AttrEntryId] = new() { S = DynamoDbOutboxContext.GroupEntryId },
                        [DynamoDbOutboxContext.AttrCreatedAt] = new() { N = _createdAtEpoch },
                    }
                }
            });
        }

        var payload = serializer.Serialize(context.Message);
        var headersJson = DynamoDbOutboxContext.SerializeHeaders(context.Headers);

        var msgItem = new Dictionary<string, AttributeValue>
        {
            [DynamoDbOutboxContext.AttrGroupId] = new() { S = _groupId },
            [DynamoDbOutboxContext.AttrEntryId] = new() { S = Guid.CreateVersion7().ToString() },
            [DynamoDbOutboxContext.AttrDestination] = new() { S = context.Destination.ToString() },
            [DynamoDbOutboxContext.AttrPayload] = new() { B = new MemoryStream(payload) },
            [DynamoDbOutboxContext.AttrHeadersJson] = new() { S = headersJson },
            [DynamoDbOutboxContext.AttrCreatedAt] = new() { N = _createdAtEpoch },
        };

        if (context.Delay.HasValue)
            msgItem[DynamoDbOutboxContext.AttrDelay] = new AttributeValue
                { N = ((long)context.Delay.Value.TotalSeconds).ToString() };

        outboxContext.AddTransactItem(new TransactWriteItem
        {
            Put = new Put
            {
                TableName = options.TableName,
                Item = msgItem,
            }
        });

        return Task.CompletedTask;
    }
}
