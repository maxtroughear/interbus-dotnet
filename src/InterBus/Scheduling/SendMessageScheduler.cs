using InterBus.Publishing;

namespace InterBus.Scheduling;

/// <summary>
/// <see cref="IScheduler"/> implementation that asynchronously schedules messages by
/// sending a concrete command message to the bus. The command is consumed by
/// <see cref="SchedulingConsumer"/>, which enlists the registered
/// <see cref="IMessageSchedulingProvider"/> to perform the actual deferred delivery.
/// <para>
/// Register via <c>bus.AddPublishScheduling()</c>. Also register the consumer that processes
/// the commands via <c>bus.AddSchedulingConsumer()</c> - this may be in the same application
/// or in a dedicated consumer service.
/// </para>
/// </summary>
internal sealed class SendMessageScheduler(
    IPublisher publisher,
    IMessageSerializer serializer)
    : IMessageScheduler
{
    public async Task<Guid> ScheduleInternalAsync<T>(PublishContext<T> context, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken) where T : class
    {
        var scheduleId = Guid.CreateVersion7();
        var payload = serializer.Serialize(context.Message);
        var command = new ScheduleDeliveryCommand
        {
            ScheduleId = scheduleId,
            Destination = context.Destination.ToString(),
            Payload = payload,
            Headers = new Dictionary<string, string>(context.Headers),
            ScheduledAt = scheduledAt
        };
        await publisher.SendAsync<SchedulingConsumer, ScheduleDeliveryCommand>(command, cancellationToken);
        return scheduleId;
    }

    public Task CancelInternalAsync(Guid scheduleId, CancellationToken cancellationToken) =>
        publisher.SendAsync<SchedulingConsumer, CancelDeliveryCommand>(
            new CancelDeliveryCommand { ScheduleId = scheduleId }, cancellationToken);
}
