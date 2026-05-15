using InterBus.Consuming;
using InterBus.Dispatching;
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

    public async Task<TReply> ReceiveAsync<TReply>(CancellationToken cancellationToken = default)
        where TReply : class
    {
        var expectedType = RoutingConventions.TopicName(typeof(TReply));

        await foreach (var envelope in queue.ReadAllAsync(cancellationToken))
        {
            // Skip messages whose message-type header doesn't match the expected reply type.
            if (envelope.Headers.TryGetValue(InterBusHeaders.MessageType, out var msgType) &&
                !string.Equals(msgType, expectedType, StringComparison.Ordinal))
                continue;

            var tcs = new TaskCompletionSource<TReply>(TaskCreationOptions.RunContinuationsAsynchronously);

            var descriptor = ConsumerDescriptor.CreateForReply<TReply>(
                Address,
                reply =>
                {
                    tcs.TrySetResult(reply);
                    return Task.CompletedTask;
                });

            try
            {
                await using var scope = scopeFactory.CreateAsyncScope();
                var executor = scope.ServiceProvider.GetRequiredService<DispatchPipelineExecutor>();
                var context = new DispatchContext(descriptor, envelope.Payload, envelope.Headers);
                await executor.ExecuteAsync(context, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                tcs.TrySetException(ex);
            }

            if (tcs.Task.IsCompleted)
                return await tcs.Task;
        }

        cancellationToken.ThrowIfCancellationRequested();
        throw new InvalidOperationException(
            $"InterBus InMemory: reply channel closed without receiving a {typeof(TReply).Name}");
    }

    public ValueTask DisposeAsync()
    {
        broker.RemoveTemporaryQueue(_queueName);
        queue.Complete();
        return ValueTask.CompletedTask;
    }
}
