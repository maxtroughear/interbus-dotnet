using InterBus.Middleware;

namespace InterBus.Scheduler;

/// <summary>
/// <see cref="IConsumerSchedulerFactory"/> for the publish-based scheduling path
/// (<c>AddPublishScheduling()</c>). Creates a <see cref="PublishConsumerScopedMessageScheduler"/>
/// that forwards scheduling requests as a <see cref="ScheduleDeliveryCommand"/> via the
/// consumer-scoped <see cref="IMessagePublisher"/>, ensuring consumer-keyed
/// <see cref="ISendMiddleware"/> and <see cref="IPublishMiddleware"/> steps run.
/// </summary>
internal sealed class PublishConsumerSchedulerFactory(InterBusOptions options) : IConsumerSchedulerFactory
{
    public IMessageScheduler? CreateForConsumer(
        IMiddlewareFactory middlewareFactory,
        IMessageSerializer serializer,
        Type consumerType,
        IMessagePublisher consumerPublisher,
        IMessageSchedulingProvider? _,
        IConsumeContextAccessor contextAccessor) =>
        new PublishConsumerScopedMessageScheduler(
            options, middlewareFactory, serializer, consumerType, contextAccessor, consumerPublisher);
}
