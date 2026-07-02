using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.Requests;

/// <summary>
/// Shared <see cref="IReplyEndpoint"/> implementation for transports that route replies through
/// a host-managed virtual queue.
/// </summary>
public sealed class VirtualQueueReplyEndpoint(
    IVirtualQueueHost host,
    IServiceScopeFactory scopeFactory,
    Uri address,
    string virtualQueueId,
    ChannelReader<(byte[] Payload, MessageHeaders Headers)> reader)
    : IReplyEndpoint
{
    public Uri Address { get; } = address;

    public Task<TReply> ReceiveAsync<TReply>(CancellationToken cancellationToken = default)
        where TReply : class
        => ReplyEndpointDispatcher.ReceiveAsync<TReply>(
            Address,
            scopeFactory,
            reader.ReadAllAsync(cancellationToken),
            cancellationToken);

    public ValueTask DisposeAsync()
    {
        host.Unregister(virtualQueueId);
        return ValueTask.CompletedTask;
    }
}
