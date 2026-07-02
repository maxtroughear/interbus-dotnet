using System.Runtime.CompilerServices;
using InterBus.Consuming;
using InterBus.Requests;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.InMemory;

/// <summary>
/// In-memory implementation of <see cref="IReplyEndpoint"/>.
/// Dispatches incoming messages from a temporary <see cref="InMemoryQueue"/> through the
/// full consume pipeline. Messages whose <see cref="InterBusHeaders.MessageType"/> header
/// does not match the expected reply type are silently skipped.
/// </summary>
internal sealed class InMemoryReplyEndpoint(
    InMemoryBroker broker,
    IServiceScopeFactory scopeFactory,
    Uri address,
    InMemoryQueue queue)
    : IReplyEndpoint
{
    private readonly string _queueName = RoutingConventions.GetDestinationName(address);

    public Uri Address { get; } = address;

    public Task<TReply> ReceiveAsync<TReply>(CancellationToken cancellationToken = default)
        where TReply : class
        => ReplyEndpointDispatcher.ReceiveAsync<TReply>(
            Address,
            scopeFactory,
            ReadMessages(cancellationToken),
            $"InterBus InMemory: reply channel closed without receiving a {typeof(TReply).Name}",
            cancellationToken);

    private async IAsyncEnumerable<(byte[] Payload, MessageHeaders Headers)> ReadMessages(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var envelope in queue.ReadAllAsync(cancellationToken))
            yield return (envelope.Payload, envelope.Headers);
    }

    public ValueTask DisposeAsync()
    {
        broker.RemoveTemporaryQueue(_queueName);
        queue.Complete();
        return ValueTask.CompletedTask;
    }
}
