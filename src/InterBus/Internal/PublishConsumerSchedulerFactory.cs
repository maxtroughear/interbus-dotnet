using InterBus.Middleware;

namespace InterBus.Internal;

/// <summary>
/// <see cref="IConsumerSchedulerFactory"/> for the publish-based scheduling path
/// (<c>AddPublishScheduling()</c>). Creates a <see cref="ConsumerScopedMessageScheduler"/>
/// that forwards scheduling requests as a <see cref="ScheduleDeliveryCommand"/> via the
/// consumer-scoped <see cref="IMessagePublisher"/>, ensuring consumer-keyed
/// <see cref="ISendMiddleware"/> and <see cref="IPublishMiddleware"/> steps run.
/// </summary>
internal sealed class PublishConsumerSchedulerFactory(InterBusOptions options) : IConsumerSchedulerFactory
{
    public ConsumerScopedMessageScheduler? CreateForConsumer(
        IMiddlewareFactory middlewareFactory,
        IMessageSerializer serializer,
        Type consumerType,
        IMessagePublisher consumerPublisher,
        IMessageSchedulingProvider? _) =>
        new(options, middlewareFactory, serializer, consumerType, consumerPublisher,
            isPublishBased: true, provider: null);
}
