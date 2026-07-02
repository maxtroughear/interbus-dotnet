using System.Threading.Channels;
using InterBus.Requests;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.RabbitMQ;

/// <summary>
/// RabbitMQ implementation of <see cref="IReplyEndpoint"/>.
/// Reads from the in-memory channel allocated by <see cref="RabbitMqVirtualQueueHost"/> for this
/// exchange; no per-request RabbitMQ queue is created. On disposal the virtual queue is
/// unregistered from the host so its channel is completed and resources released.
/// </summary>
internal sealed class RabbitMqReplyEndpoint(
    RabbitMqVirtualQueueHost host,
    IServiceScopeFactory scopeFactory,
    Uri address,
    string vqId,
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
            $"InterBus RabbitMQ: reply channel closed without receiving a {typeof(TReply).Name}",
            cancellationToken);

    public ValueTask DisposeAsync()
    {
        host.Unregister(vqId);
        return ValueTask.CompletedTask;
    }
}
