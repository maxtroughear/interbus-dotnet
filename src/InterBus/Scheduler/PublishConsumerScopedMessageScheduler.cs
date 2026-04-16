using InterBus.Middleware;

namespace InterBus.Scheduler;

/// <summary>
/// Consumer-scoped <see cref="IMessageScheduler"/> for the publish-based scheduling path
/// (<c>AddPublishScheduling()</c>). Forwards the serialized message as a
/// <see cref="ScheduleDeliveryCommand"/> via the consumer-scoped
/// <see cref="IMessagePublisher"/>, ensuring consumer-keyed
/// <see cref="ISendMiddleware"/> and <see cref="IPublishMiddleware"/> steps run when a
/// consumer schedules a message from within its <c>Consume</c> method.
/// </summary>
internal sealed class PublishConsumerScopedMessageScheduler(
    InterBusOptions options,
    IMiddlewareFactory middlewareFactory,
    IMessageSerializer serializer,
    Type consumerType,
    IConsumeContextAccessor contextAccessor,
    IMessagePublisher consumerPublisher)
    : ConsumerScopedMessageSchedulerBase(options, middlewareFactory, serializer, consumerType, contextAccessor)
{
    protected override Func<ScheduleContext, CancellationToken, Task> BuildTerminal(DateTimeOffset scheduledAt) =>
        (ctx, ct) =>
        {
            var command = new ScheduleDeliveryCommand
            {
                Destination = ctx.Destination.ToString(),
                Payload = ctx.Payload,
                Headers = new Dictionary<string, string>(ctx.Headers),
                ScheduledAt = scheduledAt
            };
            return consumerPublisher.SendAsync<SchedulingConsumer, ScheduleDeliveryCommand>(command, ct);
        };
}
