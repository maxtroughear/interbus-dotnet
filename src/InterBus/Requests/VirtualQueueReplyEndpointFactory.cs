using Microsoft.Extensions.DependencyInjection;

namespace InterBus.Requests;

/// <summary>
/// Shared <see cref="IReplyEndpointFactory"/> for transports that expose replies through a
/// shared virtual queue host.
/// </summary>
public sealed class VirtualQueueReplyEndpointFactory(
    IVirtualQueueHost host,
    InterBusOptions options,
    IServiceScopeFactory scopeFactory)
    : IReplyEndpointFactory
{
    public async Task<IReplyEndpoint> CreateAsync(CancellationToken cancellationToken = default)
    {
        var registration = await host.RegisterAsync(cancellationToken);

        var address = new UriBuilder(RoutingConventions.QueueUri(options.QueueUriScheme, registration.QueueName))
        {
            Query = $"vq={Uri.EscapeDataString(registration.VirtualQueueId)}"
        }.Uri;

        return new VirtualQueueReplyEndpoint(
            host,
            scopeFactory,
            address,
            registration.VirtualQueueId,
            registration.Reader);
    }
}
