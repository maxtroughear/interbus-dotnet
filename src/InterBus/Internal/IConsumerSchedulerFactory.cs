using InterBus.Middleware;

namespace InterBus.Internal;

/// <summary>
/// Factory registered when scheduling is configured (<c>AddScheduling()</c> or
/// <c>AddPublishScheduling()</c>). Used by <see cref="ConsumePipelineExecutor"/> to
/// create a consumer-scoped <see cref="IMessageScheduler"/> without having to
/// directly depend on <see cref="IMessageScheduler"/> (which may itself have optional
/// dependencies such as <see cref="IMessageSchedulingProvider"/> that are not always registered).
/// </summary>
/// <remarks>
/// This interface is part of the InterBus infrastructure and is not intended for direct use by
/// application code. Implement only when integrating a custom scheduling mechanism.
/// </remarks>
public interface IConsumerSchedulerFactory
{
    /// <summary>
    /// Creates an <see cref="IMessageScheduler"/> for the given consumer, or returns
    /// <c>null</c> if the required runtime dependencies (e.g. a scheduling provider) are not
    /// available in the current scope.
    /// </summary>
    /// <param name="middlewareFactory">The middleware factory for resolving consumer-keyed middleware.</param>
    /// <param name="serializer">The message serializer.</param>
    /// <param name="consumerType">The type of the consuming consumer.</param>
    /// <param name="consumerPublisher">The consumer-scoped publisher for the consuming consumer.</param>
    /// <param name="provider">The scheduling provider, if registered.</param>
    /// <param name="contextAccessor">
    /// The consume context accessor for the current scope. Used to propagate
    /// <see cref="InterBusHeaders.CorrelationId"/> and to set
    /// <see cref="InterBusHeaders.CausationId"/> on scheduled messages.
    /// </param>
    IMessageScheduler? CreateForConsumer(
        IMiddlewareFactory middlewareFactory,
        IMessageSerializer serializer,
        Type consumerType,
        IMessagePublisher consumerPublisher,
        IMessageSchedulingProvider? provider,
        IConsumeContextAccessor contextAccessor);
}
