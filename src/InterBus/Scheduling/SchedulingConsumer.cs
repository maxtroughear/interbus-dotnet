namespace InterBus.Scheduling;

/// <summary>
/// Consumer that processes <see cref="ScheduleDeliveryCommand"/> messages published by
/// <see cref="PublishMessageScheduler"/> or <see cref="RequestMessageScheduler"/>. Enlists the
/// registered <see cref="IMessageSchedulingProvider"/> to perform the actual deferred delivery.
/// <para>
/// After a schedule is created, publishes a <see cref="ScheduleCreatedEvent"/> carrying the
/// provider-generated schedule identifier. If the incoming command carried a
/// <see cref="InterBusHeaders.ReplyTo"/> header (i.e. the caller used the request/reply
/// flow via <see cref="RequestMessageScheduler"/>), the event is also sent directly to the
/// reply address so the caller can obtain the schedule identifier synchronously.
/// </para>
/// <para>
/// Register via <c>bus.AddSchedulingConsumer()</c> alongside an
/// <see cref="IMessageSchedulingProvider"/> (e.g. via <c>AddTickerQScheduler()</c>).
/// </para>
/// </summary>
internal sealed class SchedulingConsumer(IMessageSchedulingProvider provider, IPublisher publisher) :
    IConsumer<ScheduleDeliveryCommand>,
    IConsumer<CancelDeliveryCommand>
{
    public async Task Consume(ScheduleDeliveryCommand message, IConsumeContext context,
        CancellationToken cancellationToken = default)
    {
        var scheduleId = await provider.ScheduleAsync(
            new Uri(message.Destination),
            message.Payload,
            new MessageHeaders(message.Headers),
            message.ScheduledAt,
            cancellationToken);

        var @event = new ScheduleCreatedEvent { ScheduleId = scheduleId };

        await publisher.PublishAsync(@event, cancellationToken);

        if (context.ReplyTo is { } replyTo)
            await publisher.SendAsync(replyTo, @event, cancellationToken);
    }

    public Task Consume(CancelDeliveryCommand message, IConsumeContext context,
        CancellationToken cancellationToken = default) =>
        provider.CancelAsync(message.ScheduleId, cancellationToken);
}
