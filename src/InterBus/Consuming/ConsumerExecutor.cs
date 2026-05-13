using System.Diagnostics;
using InterBus.Telemetry;

namespace InterBus.Consuming;

/// <summary>
/// DI-registered consumer executor responsible for invoking the app's
/// <see cref="IConsumer{TMessage}.Consume"/> method within a traced child activity.
/// </summary>
/// <typeparam name="TConsumer">The consumer type</typeparam>
/// <typeparam name="TMessage">The message type</typeparam>
public sealed class ConsumerExecutor<TConsumer, TMessage>(IInterBusDiagnostics diagnostics, TConsumer consumer)
    : IConsumerExecutor<TConsumer, TMessage>
    where TConsumer : IConsumer<TMessage>
    where TMessage : class
{
    /// <summary>
    /// Invokes the consumer's <see cref="IConsumer{TMessage}.Consume"/> method within a
    /// child <see cref="Activity"/> so that app-code latency is visible as a separate span.
    /// Sets <see cref="ActivityStatusCode.Error"/> on the activity if the consumer throws
    /// (except for <see cref="OperationCanceledException"/>).
    /// </summary>
    public async Task ExecuteAsync(
        ConsumeContext<TMessage> context,
        CancellationToken cancellationToken)
    {
        using var activity = diagnostics.ActivitySource.StartActivity($"{typeof(TConsumer).Name} consume");

        try
        {
            await consumer.Consume(context.Message, context, cancellationToken);
            activity?.SetStatus(ActivityStatusCode.Ok);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.AddException(ex);
            throw;
        }
    }
}
