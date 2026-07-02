using System.Threading.Channels;
using InterBus.Requests;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.AzureServiceBus;

/// <summary>
/// Azure Service Bus implementation of <see cref="IReplyEndpoint"/>.
/// Reads from the in-memory channel allocated by <see cref="AzureServiceBusSessionQueueHost"/>
/// for this exchange; no per-request ASB queue is created. On disposal the session channel is
/// unregistered so its resources are released.
/// </summary>
internal sealed class AzureServiceBusReplyEndpoint(
    AzureServiceBusSessionQueueHost host,
    IServiceScopeFactory scopeFactory,
    Uri address,
    string sessionId,
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
            $"InterBus AzureServiceBus: reply channel for session '{sessionId}' closed without receiving a {typeof(TReply).Name} — the channel was completed without delivering a message (possible causes: reply endpoint was disposed early, or the session queue host was shut down)",
            cancellationToken);

    public ValueTask DisposeAsync()
    {
        host.Unregister(sessionId);
        return ValueTask.CompletedTask;
    }
}
