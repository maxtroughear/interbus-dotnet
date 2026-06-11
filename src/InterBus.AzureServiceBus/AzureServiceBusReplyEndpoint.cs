using System.Threading.Channels;
using InterBus.Consuming;
using InterBus.Dispatching;
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

    public async Task<TReply> ReceiveAsync<TReply>(CancellationToken cancellationToken = default)
        where TReply : class
    {
        await foreach (var (payload, headers) in reader.ReadAllAsync(cancellationToken))
        {
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
                var context = new DispatchContext(descriptor, payload, headers);
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
            $"InterBus AzureServiceBus: reply channel for session '{sessionId}' closed without receiving " +
            $"a {typeof(TReply).Name} — the channel was completed without delivering a message " +
            "(possible causes: reply endpoint was disposed early, or the session queue host was shut down)");
    }

    public ValueTask DisposeAsync()
    {
        host.Unregister(sessionId);
        return ValueTask.CompletedTask;
    }
}
