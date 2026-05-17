using InterBus.Consuming;
using InterBus.Requests;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.InMemory;

/// <summary>
/// In-memory implementation of <see cref="IReplyEndpointFactory"/>.
/// Creates a temporary <see cref="InMemoryQueue"/> for each request/reply exchange.
/// </summary>
internal sealed class InMemoryReplyEndpointFactory(
    InMemoryBroker broker,
    InterBusOptions options,
    IServiceScopeFactory scopeFactory)
    : IReplyEndpointFactory
{
    public Task<IReplyEndpoint> CreateAsync(CancellationToken cancellationToken = default)
    {
        var queueName = $"reply-{Guid.CreateVersion7()}";
        var address = RoutingConventions.QueueUri(options.QueueUriScheme, queueName);

        // Create a minimal descriptor so the broker can index the queue by name.
        // The Dispatch delegate is never invoked via this descriptor —
        // InMemoryReplyEndpoint creates per-reply descriptors in ReceiveAsync<TReply>.
        var descriptor = new ConsumerDescriptor(typeof(InMemoryReplyEndpointFactory), address);
        var queue = broker.CreateTemporaryQueue(descriptor);

        return Task.FromResult<IReplyEndpoint>(
            new InMemoryReplyEndpoint(broker, scopeFactory, address, queue));
    }
}
