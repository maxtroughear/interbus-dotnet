using InterBus.Requests;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.AmazonSqs;

/// <summary>
/// Amazon SQS implementation of <see cref="IReplyEndpointFactory"/>.
/// Registers a virtual queue with <see cref="SqsVirtualQueueHost"/> instead of creating a
/// real SQS queue per request, eliminating per-exchange queue create/delete latency.
/// </summary>
internal sealed class SqsReplyEndpointFactory(
    SqsVirtualQueueHost host,
    IServiceScopeFactory scopeFactory)
    : IReplyEndpointFactory
{
    public async Task<IReplyEndpoint> CreateAsync(CancellationToken cancellationToken = default)
    {
        var (hostQueueName, vqId, reader) = await host.RegisterAsync(cancellationToken);

        // The address encodes the host queue name (so SqsTransport routes to the right real
        // SQS queue) and the virtual queue ID as a query parameter (so SqsTransport injects
        // it as a header, enabling SqsVirtualQueueHost to route the reply to this channel).
        var address = new Uri(
            $"{SqsTransport.QueueScheme}://{RoutingConventions.BusHost}/{Uri.EscapeDataString(hostQueueName)}?vq={vqId}");

        return new SqsReplyEndpoint(host, scopeFactory, address, vqId, reader);
    }
}

