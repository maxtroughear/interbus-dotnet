using InterBus.Requests;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.PostgreSQL;

/// <summary>
/// PostgreSQL implementation of <see cref="IReplyEndpointFactory"/>.
/// Uses <see cref="PostgreSqlVirtualQueueHost"/> to route replies via a shared PostgreSQL
/// queue so that no per-request queue is created or deleted.
/// </summary>
internal sealed class PostgreSqlReplyEndpointFactory(
    PostgreSqlVirtualQueueHost host,
    IServiceScopeFactory scopeFactory)
    : IReplyEndpointFactory
{
    public async Task<IReplyEndpoint> CreateAsync(CancellationToken cancellationToken = default)
    {
        var (hostQueueUri, vqId, reader) = await host.RegisterAsync(cancellationToken);

        var uri = new UriBuilder(hostQueueUri)
        {
            Query = $"vq={vqId}"
        }.Uri;

        return new PostgreSqlReplyEndpoint(host, scopeFactory, uri, vqId, reader);
    }
}
