using InterBus.Publishing;

namespace InterBus.Scheduling;

/// <summary>
/// <see cref="IScheduler"/> implementation that schedules messages by publishing a
/// <see cref="ScheduleDeliveryCommand"/> to the bus (fire-and-forget). The command is
/// consumed by <see cref="SchedulingConsumer"/>, which enlists the registered
/// <see cref="IMessageSchedulingProvider"/> to perform the actual deferred delivery.
/// <para>
/// Always returns <c>null</c> from <c>Schedule*Async</c> because the provider-generated
/// schedule ID is not synchronously available on the publish path. Use
/// <see cref="RequestMessageScheduler"/> (<c>ViaRequest()</c>) when a schedule ID is required.
/// </para>
/// <para>
/// Register via <c>builder.ViaPublish()</c>. Also register the consumer that processes
/// the commands via <c>builder.AddSchedulingConsumer()</c> — this may be in the same
/// application or a dedicated consumer service.
/// </para>
/// </summary>
internal sealed class PublishMessageScheduler(
    IPublisher publisher,
    IMessageSerializer serializer)
    : IMessageScheduler
{
    public async Task<string?> ScheduleInternalAsync<T>(PublishContext<T> context, DateTimeOffset scheduledAt,
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

        await publisher.PublishAsync(command, cancellationToken);
        return null;
    }

    public Task CancelInternalAsync(string scheduleId, CancellationToken cancellationToken) =>
        publisher.PublishAsync(new CancelDeliveryCommand { ScheduleId = scheduleId }, cancellationToken);
}
