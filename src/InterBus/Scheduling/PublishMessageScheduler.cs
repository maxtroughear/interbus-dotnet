using InterBus.Publishing;

namespace InterBus.Scheduling;

/// <summary>
/// <see cref="IScheduler"/> implementation that asynchronously schedules messages by
/// publishing a concrete command message to the bus. The command is consumed by
/// <see cref="SchedulingConsumer"/>, which enlists the registered
/// <see cref="IMessageSchedulingProvider"/> to perform the actual deferred delivery.
/// <para>
/// Register via <c>bus.AddPublishScheduling()</c>. Also register the consumer that processes
/// the commands via <c>bus.AddSchedulingConsumer()</c> - this may be in the same application
/// or in a dedicated consumer service.
/// </para>
/// <para>
/// Cancellation is not supported by this scheduler. <see cref="CancelInternalAsync"/> always
/// throws <see cref="NotSupportedException"/>.
/// </para>
/// </summary>
internal sealed class PublishMessageScheduler(
    IPublisher publisher,
    IMessageSerializer serializer)
    : IMessageScheduler
{
    public async Task<string> ScheduleInternalAsync<T>(PublishContext<T> context, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken) where T : class
    {
        var payload = serializer.Serialize(context.Message);
        var command = new ScheduleDeliveryCommand
        {
            Destination = context.Destination.ToString(),
            Payload = payload,
            Headers = new Dictionary<string, string>(context.Headers),
            ScheduledAt = scheduledAt
        };
        await publisher.SendAsync<SchedulingConsumer, ScheduleDeliveryCommand>(command, cancellationToken);
        return Guid.CreateVersion7().ToString();
    }

    public Task CancelInternalAsync(string scheduleId, CancellationToken cancellationToken) =>
        throw new NotSupportedException(
            "The publish-based scheduler does not support cancellation. " +
            "Use a scheduler that supports cancellation (e.g. TickerQ or EventBridge).");
}
