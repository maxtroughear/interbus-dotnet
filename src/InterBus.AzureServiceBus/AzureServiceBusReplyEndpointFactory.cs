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
        var (rawQueueName, sessionId, reader) = await host.RegisterAsync(cancellationToken);

        // Encode the session ID as a URI query parameter so AzureServiceBusQueueTransport
        // can extract and set ServiceBusMessage.SessionId without consumers needing to know
        // about session mechanics.
        var address = new Uri(
            $"{AzureServiceBusQueueTransport.QueueScheme}://{RoutingConventions.BusHost}" +
            $"/{Uri.EscapeDataString(rawQueueName)}" +
            $"?session={Uri.EscapeDataString(sessionId)}");

        return new AzureServiceBusReplyEndpoint(host, scopeFactory, address, sessionId, reader);
    }
}
