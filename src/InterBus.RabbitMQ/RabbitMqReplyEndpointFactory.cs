using InterBus.Requests;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.RabbitMQ;

/// <summary>
/// RabbitMQ implementation of <see cref="IReplyEndpointFactory"/>.
/// Registers a virtual queue with <see cref="RabbitMqVirtualQueueHost"/> instead of creating a
/// real RabbitMQ queue per request, eliminating per-exchange queue create/delete latency.
/// </summary>
internal sealed class RabbitMqReplyEndpointFactory(
    RabbitMqVirtualQueueHost host,
    IServiceScopeFactory scopeFactory)
    : IReplyEndpointFactory
{
    public async Task<IReplyEndpoint> CreateAsync(CancellationToken cancellationToken = default)
    {
        var (queueName, vqId, reader) = await host.RegisterAsync(cancellationToken);

        var address = new UriBuilder(
            RoutingConventions.QueueUri(RabbitMqQueueTransport.QueueScheme, queueName))
        {
            Query = $"vq={vqId}"
        }.Uri;

        return new RabbitMqReplyEndpoint(host, scopeFactory, address, vqId, reader);
    }
}
