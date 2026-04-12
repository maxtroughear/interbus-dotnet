using InterBus.Middleware;

namespace InterBus.Internal;

/// <summary>
/// <see cref="IConsumerSchedulerFactory"/> for the direct scheduling path
/// (<c>AddScheduling()</c>). Creates a <see cref="DirectConsumerScopedMessageScheduler"/> that
/// forwards scheduling requests directly to the registered
/// <see cref="IMessageSchedulingProvider"/>, running consumer-keyed
/// <see cref="IScheduleMiddleware"/> steps before the provider call.
/// Returns <c>null</c> when <paramref name="provider"/> is <c>null</c> (no provider
/// registered in the current scope), which causes the
/// <see cref="ConsumeContext"/> to have no scheduler and throw an informative exception
/// only if a consumer actually calls a scheduling method.
/// </summary>
internal sealed class DirectConsumerSchedulerFactory(InterBusOptions options) : IConsumerSchedulerFactory
{
    public IMessageScheduler? CreateForConsumer(
        IMiddlewareFactory middlewareFactory,
        IMessageSerializer serializer,
        Type consumerType,
        IMessagePublisher consumerPublisher,
        IMessageSchedulingProvider? provider,
        IConsumeContextAccessor contextAccessor)
    {
        if (provider == null)
            return null;

        return new DirectConsumerScopedMessageScheduler(
            options, middlewareFactory, serializer, consumerType, contextAccessor, provider);
    }
}

