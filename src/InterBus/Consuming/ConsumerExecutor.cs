using System.Diagnostics;
using InterBus.Telemetry;

namespace InterBus.Consuming;

/// <summary>
/// DI-registered consumer executor responsible for invoking the app's
/// <see cref="IConsumer{TMessage}.Consume"/> method within a traced child activity.
/// Registered as a scoped service by <see cref="IInterBusBuilder.AddConsumer{TConsumer}"/>;
/// dependencies are resolved from the container without any runtime reflection in the hot path.
/// </summary>
/// <typeparam name="TConsumer">The consumer type.</typeparam>
public sealed class ConsumerExecutor<TConsumer>(IInterBusDiagnostics diagnostics)
    where TConsumer : class, IConsumer
{
    /// <summary>
    /// Invokes the consumer's <see cref="IConsumer{TMessage}.Consume"/> method within a
    /// child <see cref="Activity"/> so that app-code latency is visible as a separate span.
    /// Sets <see cref="ActivityStatusCode.Error"/> on the activity if the consumer throws
    /// (except for <see cref="OperationCanceledException"/>).
    /// </summary>
    public async Task ExecuteAsync<TMessage>(
        TConsumer consumer,
        TMessage message,
        IConsumeContext context,
        CancellationToken cancellationToken)
        where TMessage : class
    {
        using var activity = diagnostics.ActivitySource.StartActivity(
            $"{typeof(TConsumer).Name} consume",
            ActivityKind.Internal);
        try
        {
            // Safe: the source generator only calls ExecuteAsync<TMessage> when TConsumer
            // implements IConsumer<TMessage>.
            await ((IConsumer<TMessage>)consumer).Consume(message, context, cancellationToken);
            activity?.SetStatus(ActivityStatusCode.Ok);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            throw;
        }
    }
}
