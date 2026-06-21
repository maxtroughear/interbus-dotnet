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
        var (hostAddress, vqId, reader) = await host.RegisterAsync(cancellationToken);

        var uriBuilder = new UriBuilder(hostAddress)
        {
            Query = $"vq={vqId}"
        };

        return new SqsReplyEndpoint(host, scopeFactory, uriBuilder.Uri, vqId, reader);
    }
}
