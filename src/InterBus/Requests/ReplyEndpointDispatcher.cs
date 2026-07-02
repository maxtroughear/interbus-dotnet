using InterBus.Consuming;
using InterBus.Dispatching;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.Requests;

/// <summary>
/// Shared reply dispatch logic used by transport-specific <see cref="IReplyEndpoint"/> implementations.
/// </summary>
public static class ReplyEndpointDispatcher
{
    /// <summary>
    /// Reads reply payloads and dispatches them through the consume pipeline until a
    /// <typeparamref name="TReply"/> is produced.
    /// </summary>
    public static async Task<TReply> ReceiveAsync<TReply>(
        Uri address,
        IServiceScopeFactory scopeFactory,
        IAsyncEnumerable<(byte[] Payload, MessageHeaders Headers)> messages,
        string channelClosedError,
        CancellationToken cancellationToken = default)
        where TReply : class
    {
        await foreach (var (payload, headers) in messages)
        {
            var tcs = new TaskCompletionSource<TReply>(TaskCreationOptions.RunContinuationsAsynchronously);

            var descriptor = ConsumerDescriptor.CreateForReply<TReply>(
                address,
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
        throw new InvalidOperationException(channelClosedError);
    }
}
