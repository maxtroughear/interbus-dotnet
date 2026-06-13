using System.IO.Hashing;
using System.Text;
using System.Text.Json;
using InterBus.DeadLettering;
using InterBus.Publishing;
using Microsoft.EntityFrameworkCore;

namespace InterBus.EntityFrameworkCore.Outbox;

/// <summary>
/// Publish middleware that writes every outgoing message to the transactional outbox instead of
/// forwarding it to the transport directly. The message is only sent once the caller commits its
/// database transaction, guaranteeing atomicity with any surrounding business-logic writes.
/// <para>
/// Messages are partitioned by <see cref="InterBusHeaders.MessageGroupId"/> within a scope:
/// each group ID gets its own <see cref="OutboxGroup"/>. This allows one transaction to enqueue
/// multiple independent FIFO groups while still committing atomically.
/// </para>
/// </summary>
internal sealed class OutboxPublishMiddleware<TDbContext>(
    TDbContext dbContext,
    IMessageSerializer serializer,
    OutboxOptions options)
    : IPublishMiddleware
    where TDbContext : DbContext
{
    private readonly Dictionary<string, OutboxGroup> _outboxGroups = new(StringComparer.Ordinal);

    public Task InvokeAsync<T>(PublishContext<T> context, Func<PublishContext<T>, CancellationToken, Task> next,
        CancellationToken cancellationToken = default) where T : class
    {
        // DeadLetter<> messages are published in the consume failure path, where no
        // SaveChangesAsync is ever called. Buffering them in the outbox would cause them to be
        // silently lost, so forward directly to the transport instead.
        if (typeof(T).IsGenericType && typeof(T).GetGenericTypeDefinition() == typeof(DeadLetter<>))
            return next(context, cancellationToken);

        var payload = serializer.Serialize(context.Message);
        var headersJson = SerializeHeaders(context.Headers);
        var messageGroupId = ResolveMessageGroupId(context.Headers);
        var outboxGroup = EnsureGroupExists(messageGroupId);

        outboxGroup.Messages.Add(new OutboxMessage
        {
            GroupId = outboxGroup.Id,
            Destination = context.Destination.ToString(),
            Payload = payload,
            HeadersJson = headersJson,
            Delay = context.Delay,
        });

        // Do not call next — messages are forwarded by OutboxProcessor (or inline delivery in
        // OutboxConsumeMiddleware for consumer-context publishes).
        return Task.CompletedTask;
    }

    private OutboxGroup EnsureGroupExists(string messageGroupId)
    {
        if (_outboxGroups.TryGetValue(messageGroupId, out var existing))
            return existing;

        // Deterministically assign shard by message group ID so all messages for the same group
        // always route to the same shard across processes/instances.
        var groupId = Guid.CreateVersion7();
        var shard = options.ShardCount > 1
            ? (int)(ComputeStableHash(messageGroupId) % (ulong)options.ShardCount)
            : 0;

        var outboxGroup = new OutboxGroup
        {
            Id = groupId,
            Shard = shard,
        };

        _outboxGroups[messageGroupId] = outboxGroup;
        dbContext.Add(outboxGroup);

        return outboxGroup;
    }

    private static string ResolveMessageGroupId(MessageHeaders headers)
    {
        if (!headers.TryGetValue(InterBusHeaders.MessageGroupId, out var messageGroupId)
            || string.IsNullOrWhiteSpace(messageGroupId))
        {
            throw new InvalidOperationException(
                $"Missing required '{InterBusHeaders.MessageGroupId}' header for EF Core outbox grouping");
        }

        return messageGroupId;
    }

    private static string SerializeHeaders(MessageHeaders headers) =>
        JsonSerializer.Serialize(headers, OutboxJsonContext.Default.DictionaryStringString);

    private static ulong ComputeStableHash(string value) =>
        XxHash3.HashToUInt64(Encoding.UTF8.GetBytes(value));
}
