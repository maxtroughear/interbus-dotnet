namespace InterBus.InMemory;

/// <summary>
/// In-memory implementation of <see cref="IReplyEndpoint{TReply}"/>.
/// Reads directly from a temporary <see cref="InMemoryQueue"/> channel, skipping any message
/// that cannot be deserialized as <typeparamref name="TReply"/>.
/// </summary>
internal sealed class InMemoryReplyEndpoint<TReply>(
    InMemoryBroker broker,
    IMessageSerializer serializer,
    Uri address,
    InMemoryQueue queue)
    : IReplyEndpoint<TReply>
    where TReply : class
{
    private readonly string _queueName = RoutingConventions.GetDestinationName(address);

    public Uri Address { get; } = address;

    public async Task<TReply> ReceiveAsync(CancellationToken cancellationToken = default)
    {
        var expectedType = RoutingConventions.TopicName(typeof(TReply));

        await foreach (var envelope in queue.ReadAllAsync(cancellationToken))
        {
            // Skip messages whose message-type header doesn't match the expected reply type.
            if (envelope.Headers.TryGetValue(InterBusHeaders.MessageType, out var msgType) &&
                !string.Equals(msgType, expectedType, StringComparison.Ordinal))
                continue;

            try
            {
                var reply = serializer.Deserialize<TReply>(envelope.Payload);
                if (reply is not null)
                    return reply;
            }
            catch
            {
                // Deserialization failed for this message — skip it and wait for the next one.
            }
        }

        cancellationToken.ThrowIfCancellationRequested();
        throw new InvalidOperationException(
            $"InterBus InMemory: reply channel closed without receiving a {typeof(TReply).Name}");
    }

    public ValueTask DisposeAsync()
    {
        broker.RemoveTemporaryQueue(_queueName);
        return ValueTask.CompletedTask;
    }
}
