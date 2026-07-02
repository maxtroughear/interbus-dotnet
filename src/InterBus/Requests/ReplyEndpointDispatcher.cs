using System.Diagnostics;
using InterBus.Consuming;
using InterBus.Dispatching;
using InterBus.Telemetry;
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
        CancellationToken cancellationToken = default)
        where TReply : class
    {
        await foreach (var (payload, headers) in messages)
        {
            var tcs = new TaskCompletionSource<TReply>(TaskCreationOptions.RunContinuationsAsynchronously);
            var replyType = RoutingConventions.MessageTypeString(typeof(TReply));
            var destinationName = RoutingConventions.GetDestinationName(address);

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
                var diagnostics = scope.ServiceProvider.GetService<IInterBusDiagnostics>();
                var metrics = scope.ServiceProvider.GetService<InterBusMetrics>();
                var context = new DispatchContext(descriptor, payload, headers);
                var startedAt = Stopwatch.GetTimestamp();

                using var activity = diagnostics is null
                    ? null
                    : InterBusTelemetry.StartConsumerActivity(diagnostics, headers, "ReplyEndpointConsumer",
                        destinationName);

                try
                {
                    await executor.ExecuteAsync(context, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                    activity?.AddEvent(new ActivityEvent("interbus.reply.failed",
                        tags: new ActivityTagsCollection
                        {
                            { "exception.type", ex.GetType().FullName },
                            { "exception.message", ex.Message },
                        }));
                    metrics?.RecordReplyFailed(replyType, destinationName);
                    throw;
                }
                finally
                {
                    metrics?.RecordReplyReceiveDuration(
                        Stopwatch.GetElapsedTime(startedAt).TotalSeconds,
                        replyType,
                        destinationName);
                }

                switch (context.Result.Kind)
                {
                    case DispatchResultKind.Success:
                        if (tcs.Task.IsCompletedSuccessfully)
                        {
                            activity?.SetStatus(ActivityStatusCode.Ok);
                            activity?.AddEvent(new ActivityEvent("interbus.reply.received"));
                            metrics?.RecordReplyReceived(replyType, destinationName);
                        }
                        else
                        {
                            activity?.AddEvent(new ActivityEvent("interbus.reply.ignored"));
                        }
                        break;
                    case DispatchResultKind.Release:
                    case DispatchResultKind.DeadLetter:
                        activity?.SetStatus(ActivityStatusCode.Error, context.Result.Exception?.Message);
                        activity?.AddEvent(new ActivityEvent("interbus.reply.failed",
                            tags: new ActivityTagsCollection
                            {
                                { "exception.type", context.Result.Exception?.GetType().FullName },
                                { "exception.message", context.Result.Exception?.Message },
                            }));
                        metrics?.RecordReplyFailed(replyType, destinationName);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
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
            $"InterBus: reply channel closed without receiving a {typeof(TReply).Name}");
    }
}
