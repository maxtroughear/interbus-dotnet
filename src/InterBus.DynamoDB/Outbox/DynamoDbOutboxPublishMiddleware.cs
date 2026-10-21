using System.Collections.Concurrent;
using System.IO.Hashing;
using System.Text;
using Amazon.DynamoDBv2.Model;
using InterBus.DeadLettering;
using InterBus.Publishing;

namespace InterBus.DynamoDB.Outbox;

/// <summary>
/// Publish middleware that captures every outgoing message and registers it with the
/// <see cref="DynamoDbOutboxContext"/> instead of forwarding it to the transport directly.
/// The message is only sent once the caller invokes
/// <see cref="IDynamoDbOutboxContext.CommitAsync"/>, which writes the outbox items together
/// with any application-provided <c>TransactWriteItem</c>s atomically via DynamoDB
/// <c>TransactWriteItems</c>.
/// </summary>
internal sealed class DynamoDbOutboxPublishMiddleware(
    DynamoDbOutboxContext outboxContext,
    DynamoDbOutboxOptions options,
    IMessageSerializer serializer)
    : IPublishMiddleware
{
    private readonly ConcurrentDictionary<string, byte> _groups = new();
    private readonly string _createdAtEpoch = DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString();

    public Task InvokeAsync<T>(PublishContext<T> context, Func<PublishContext<T>, CancellationToken, Task> next,
        CancellationToken cancellationToken = default) where T : class
    {
        // DeadLetter<> messages are published in the consume failure path where CommitAsync
        // is never called. Forward them directly to the transport to avoid silent loss.
        if (typeof(T).IsGenericType && typeof(T).GetGenericTypeDefinition() == typeof(DeadLetter<>))
            return next(context, cancellationToken);

        var groupId = ResolveMessageGroupId(context.Headers);

        if (_groups.TryAdd(groupId, 0))
        {
            var shardBytes = Encoding.UTF8.GetBytes(groupId);
            var shardIndex = XxHash3.HashToUInt64(shardBytes) % (ulong)options.ShardCount;

            outboxContext.AddTransactItem(new TransactWriteItem
            {
                Put = new Put
                {
                    TableName = options.TableName,
                    Item = new Dictionary<string, AttributeValue>
                    {
                        [DynamoDbOutboxContext.AttrGroupId] = new() { S = groupId },
                        [DynamoDbOutboxContext.AttrEntryId] = new() { S = DynamoDbOutboxContext.GroupEntryId },
                        [DynamoDbOutboxContext.AttrCreatedAt] = new() { N = _createdAtEpoch },
                        [DynamoDbOutboxContext.AttrShard] = new() { N = shardIndex.ToString() },
                        [DynamoDbOutboxContext.AttrVisibleAt] = new() { N = _createdAtEpoch }
                    }
                }
            });
        }

        var payload = serializer.Serialize(context.Message);
        var headersJson = DynamoDbOutboxContext.SerializeHeaders(context.Headers);

        var msgItem = new Dictionary<string, AttributeValue>
        {
            [DynamoDbOutboxContext.AttrGroupId] = new() { S = groupId },
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

    private static string ResolveMessageGroupId(MessageHeaders headers)
    {
        if (!headers.TryGetValue(InterBusHeaders.MessageGroupId, out var messageGroupId)
            || string.IsNullOrWhiteSpace(messageGroupId))
        {
            throw new InvalidOperationException(
                $"Missing required '{InterBusHeaders.MessageGroupId}' header for DynamoDB outbox grouping");
        }

        return messageGroupId;
    }
}
