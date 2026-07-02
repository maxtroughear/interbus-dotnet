using InterBus.Requests;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.AzureServiceBus;

/// <summary>
/// Azure Service Bus implementation of <see cref="IReplyEndpointFactory"/>.
/// Uses <see cref="AzureServiceBusVirtualQueueHost"/> to route replies via ASB sessions so
/// that no per-request queue is created or deleted.
/// </summary>
internal sealed class AzureServiceBusReplyEndpointFactory(
    AzureServiceBusVirtualQueueHost host,
    IServiceScopeFactory scopeFactory)
    : IReplyEndpointFactory
{
    public async Task<IReplyEndpoint> CreateAsync(CancellationToken cancellationToken = default)
    {
        var (queueName, vqId, reader) = await host.RegisterAsync(cancellationToken);

        var address =
            new UriBuilder(RoutingConventions.QueueUri(AzureServiceBusQueueTransport.QueueScheme, queueName))
            {
                Query = $"vq={Uri.EscapeDataString(vqId)}"
            }.Uri;

        return new AzureServiceBusReplyEndpoint(host, scopeFactory, address, vqId, reader);
    }
}
