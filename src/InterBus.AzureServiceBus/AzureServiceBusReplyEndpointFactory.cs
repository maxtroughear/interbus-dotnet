using InterBus.Requests;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.AzureServiceBus;

/// <summary>
/// Azure Service Bus implementation of <see cref="IReplyEndpointFactory"/>.
/// Uses <see cref="AzureServiceBusSessionQueueHost"/> to route replies via ASB sessions so
/// that no per-request queue is created or deleted.
/// </summary>
internal sealed class AzureServiceBusReplyEndpointFactory(
    AzureServiceBusSessionQueueHost host,
    IServiceScopeFactory scopeFactory)
    : IReplyEndpointFactory
{
    public async Task<IReplyEndpoint> CreateAsync(CancellationToken cancellationToken = default)
    {
        var (queueName, sessionId, reader) = await host.RegisterAsync(cancellationToken);

        var address =
            new UriBuilder(RoutingConventions.QueueUri(AzureServiceBusQueueTransport.QueueScheme, queueName))
            {
                Query = $"session={Uri.EscapeDataString(sessionId)}"
            }.Uri;

        return new AzureServiceBusReplyEndpoint(host, scopeFactory, address, sessionId, reader);
    }
}
