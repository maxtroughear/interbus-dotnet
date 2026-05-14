using InterBus.Consuming;

namespace InterBus.InMemory;

/// <summary>
/// In-memory implementation of <see cref="IReplyEndpointFactory"/>.
/// Creates a temporary <see cref="InMemoryQueue"/> for each request/reply exchange.
/// </summary>
internal sealed class InMemoryReplyEndpointFactory(
    InMemoryBroker broker,
    InterBusOptions options,
    IMessageSerializer serializer)
    : IReplyEndpointFactory
{
    public Task<IReplyEndpoint<TReply>> CreateAsync<TReply>(CancellationToken cancellationToken = default)
        where TReply : class
    {
        var queueName = $"reply-{Guid.CreateVersion7()}";
        var address = RoutingConventions.QueueUri(options.QueueUriScheme, queueName);

        // Create a minimal descriptor so the broker can index the queue by name.
        // The Dispatch delegate is never invoked — the endpoint reads from the channel directly.
        var descriptor = new ConsumerDescriptor(typeof(InMemoryReplyEndpointFactory), address);
        var queue = broker.CreateTemporaryQueue(descriptor);

        return Task.FromResult<IReplyEndpoint<TReply>>(
            new InMemoryReplyEndpoint<TReply>(broker, serializer, address, queue));
    }
}
