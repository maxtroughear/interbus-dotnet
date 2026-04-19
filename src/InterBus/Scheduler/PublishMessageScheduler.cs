using InterBus.Publisher;

namespace InterBus.Scheduler;

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
/// </summary>
internal sealed class PublishMessageScheduler(
    IPublisher publisher,
    IMessageSerializer serializer,
    InterBusOptions options)
    : IMessageScheduler
{
    public Task ScheduleInternalAsync<T>(PublishContext<T> context, DateTimeOffset scheduledAt,
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
        return publisher.SendAsync<SchedulingConsumer, ScheduleDeliveryCommand>(command, cancellationToken);
    }
}
