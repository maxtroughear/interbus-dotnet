namespace InterBus.Scheduling;

/// <summary>
/// Consumer that processes <see cref="ScheduleDeliveryCommand"/> messages published by
/// <see cref="PublishMessageScheduler"/>. Enlists the registered
/// <see cref="IMessageSchedulingProvider"/> to perform the actual deferred delivery.
/// <para>
/// Register via <c>bus.AddSchedulingConsumer()</c> alongside an
/// <see cref="IMessageSchedulingProvider"/> (e.g. via <c>AddTickerQScheduler()</c>).
/// </para>
/// </summary>
internal sealed class SchedulingConsumer(IMessageSchedulingProvider provider) :
    IConsumer<ScheduleDeliveryCommand>
{
    public Task Consume(ScheduleDeliveryCommand message, IConsumeContext context,
        CancellationToken cancellationToken = default) =>
        provider.ScheduleAsync(
            new Uri(message.Destination),
            message.Payload,
            new MessageHeaders(message.Headers),
            message.ScheduledAt,
            cancellationToken);
}
