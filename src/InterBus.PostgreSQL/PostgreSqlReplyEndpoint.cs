using System.Threading.Channels;
using InterBus.Consuming;
using InterBus.Dispatching;
using InterBus.Requests;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.PostgreSQL;

/// <summary>
/// PostgreSQL implementation of <see cref="IReplyEndpoint"/>.
/// Reads from the in-memory channel allocated by <see cref="PostgreSqlVirtualQueueHost"/> for
/// this exchange; no per-request PostgreSQL queue is created. On disposal the virtual queue is
/// unregistered from the host so its channel is completed and resources released.
/// </summary>
internal sealed class PostgreSqlReplyEndpoint(
    PostgreSqlVirtualQueueHost host,
    IServiceScopeFactory scopeFactory,
    Uri address,
    string vqId,
    ChannelReader<(byte[] Payload, MessageHeaders Headers)> reader)
    : IReplyEndpoint
{
    public Uri Address { get; } = address;

    public async Task<TReply> ReceiveAsync<TReply>(CancellationToken cancellationToken = default)
        where TReply : class
    {
        // Iterate messages from the channel. Normally exactly one message arrives per exchange.
        // If the dispatch pipeline does not invoke the reply callback (e.g. the message type
        // does not match), the tcs is not set and we continue the loop to wait for the next
        // message rather than returning prematurely. This matches the SQS and Azure patterns.
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
            $"InterBus PostgreSQL: reply channel for virtual queue '{vqId}' closed without receiving " +
            $"a {typeof(TReply).Name}");
    }

    public ValueTask DisposeAsync()
    {
        host.Unregister(vqId);
        return ValueTask.CompletedTask;
    }
}
