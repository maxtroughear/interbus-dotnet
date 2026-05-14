using InterBus.Consuming;
using InterBus.Dispatching;
using InterBus.Publishing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace InterBus.InMemory;

/// <summary>
/// In-memory implementation of <see cref="IRequestClient{TRequest,TReply}"/>.
/// Creates a temporary reply queue, sends the request to <paramref name="requestDestination"/>
/// with a <see cref="InterBusHeaders.ReplyTo"/> header pointing at the temporary queue,
/// and runs a short-lived message pump that dispatches the reply through the full
/// dispatch pipeline before completing the awaited <see cref="Task{TReply}"/>.
/// </summary>
internal sealed class InMemoryRequestClient<TRequest, TReply>(
    InMemoryBroker broker,
    IServiceScopeFactory scopeFactory,
    InterBusOptions options,
    Uri requestDestination,
    ILogger<InMemoryRequestClient<TRequest, TReply>> logger)
    : IRequestClient<TRequest, TReply>
    where TRequest : class
    where TReply : class
{
    public Task<TReply> RequestAsync(TRequest request, CancellationToken cancellationToken = default) =>
        RequestAsync(request, _ => { }, cancellationToken);

    public async Task<TReply> RequestAsync(
        TRequest request,
        Action<PublishContext<TRequest>> configure,
        CancellationToken cancellationToken = default)
    {
        var replyQueueName = $"reply-{Guid.CreateVersion7()}";
        var replyQueueUri = RoutingConventions.QueueUri(options.QueueUriScheme, replyQueueName);

        var tcs = new TaskCompletionSource<TReply>(TaskCreationOptions.RunContinuationsAsynchronously);

        // Build a temporary descriptor whose dispatch delegate captures the TCS.
        var descriptor = ConsumerDescriptor.CreateForReply(
            typeof(InMemoryRequestClient<TRequest, TReply>),
            replyQueueUri,
            (context, _, sp, ct) =>
            {
                var serializer = sp.GetRequiredService<IMessageSerializer>();
                try
                {
                    var reply = serializer.Deserialize<TReply>(context.Payload);
                    if (reply is not null)
                        tcs.TrySetResult(reply);
                    else
                        tcs.TrySetException(new InvalidOperationException(
                            $"InterBus: failed to deserialize reply of type {typeof(TReply).FullName}"));
                }
                catch (Exception ex)
                {
                    tcs.TrySetException(ex);
                }

                return Task.CompletedTask;
            });

        var replyQueue = broker.CreateTemporaryQueue(descriptor);

        try
        {
            // Send the request to the request consumer with the reply-to header.
            await using var sendScope = scopeFactory.CreateAsyncScope();
            var publisher = sendScope.ServiceProvider.GetRequiredService<IPublisher>();
            await publisher.SendAsync<TRequest>(requestDestination, request, ctx =>
            {
                configure(ctx);
                ctx.Headers[InterBusHeaders.ReplyTo] = replyQueueUri.ToString();
            }, cancellationToken);

            // Pump exactly one reply message from the temporary queue.
            await foreach (var envelope in replyQueue.ReadAllAsync(cancellationToken))
            {
                try
                {
                    await DispatchAsync(descriptor, envelope, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    tcs.TrySetCanceled(cancellationToken);
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex,
                        "InterBus InMemory: error dispatching reply for {RequestType} → {ReplyType}",
                        typeof(TRequest).Name, typeof(TReply).Name);
                    tcs.TrySetException(ex);
                }

                // One reply is enough; complete the channel so ReadAllAsync exits.
                replyQueue.Complete();
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            tcs.TrySetCanceled(cancellationToken);
        }
        finally
        {
            broker.RemoveTemporaryQueue(replyQueueName);
        }

        return await tcs.Task;
    }

    private async Task DispatchAsync(ConsumerDescriptor descriptor, InMemoryEnvelope envelope, CancellationToken ct)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var executor = scope.ServiceProvider.GetRequiredService<DispatchPipelineExecutor>();
        var context = new DispatchContext(descriptor, envelope.Payload, envelope.Headers);
        await executor.ExecuteAsync(context, ct);
    }
}
