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
    /// Creates a <see cref="ConsumerScopedMessageScheduler"/> for the given consumer, or returns
    /// <c>null</c> if the required runtime dependencies (e.g. a scheduling provider) are not
    /// available in the current scope.
    /// </summary>
    ConsumerScopedMessageScheduler? CreateForConsumer(
        IMiddlewareFactory middlewareFactory,
        IMessageSerializer serializer,
        Type consumerType,
        IMessagePublisher consumerPublisher,
        IMessageSchedulingProvider? provider);
}
