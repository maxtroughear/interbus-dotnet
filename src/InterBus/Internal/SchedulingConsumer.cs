namespace InterBus.Internal;

/// <summary>
/// Consumer that processes <see cref="SchedulePublishCommand"/> and
/// <see cref="ScheduleSendCommand"/> messages published by <see cref="PublishMessageScheduler"/>.
/// Enlists the registered <see cref="IMessageSchedulingProvider"/> to perform the actual
/// deferred delivery.
/// <para>
/// Register via <c>bus.AddSchedulingConsumer()</c> alongside an
/// <see cref="IMessageSchedulingProvider"/> (e.g. via <c>AddTickerQScheduler()</c>).
/// </para>
/// </summary>
internal sealed class SchedulingConsumer(IMessageSchedulingProvider provider) :
    IConsumer<SchedulePublishCommand>,
    IConsumer<ScheduleSendCommand>
{
    public Task Consume(SchedulePublishCommand message, IConsumeContext context,
        CancellationToken cancellationToken = default) =>
        provider.SchedulePublishToTopicAsync(
            message.TopicName,
            message.Payload,
            new MessageHeaders(message.Headers),
            message.ScheduledAt,
            cancellationToken);

    public Task Consume(ScheduleSendCommand message, IConsumeContext context,
        CancellationToken cancellationToken = default) =>
        provider.ScheduleSendToQueueAsync(
            message.QueueName,
            message.Payload,
            new MessageHeaders(message.Headers),
            message.ScheduledAt,
            cancellationToken);
}
