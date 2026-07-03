using InterBus.Publishing;

namespace InterBus.Scheduling;

/// <summary>
/// <see cref="IScheduler"/> implementation that schedules messages using request/reply semantics.
/// Sends a <see cref="ScheduleDeliveryCommand"/> via <see cref="IRequestClient{TRequest,TReply}"/>
/// and awaits a <see cref="ScheduleCreatedEvent"/> reply from the <see cref="SchedulingConsumer"/>,
/// returning the provider-generated schedule ID synchronously.
/// <para>
/// Register via <c>builder.ViaRequest()</c>. Also register the consumer that processes
/// the commands via <c>builder.AddSchedulingConsumer()</c> — this may be in the same
/// application or a dedicated consumer service.
/// </para>
/// <para>
/// Requires the transport to support request/reply (i.e. an
/// <see cref="Requests.IReplyEndpointFactory"/> must be registered).
/// </para>
/// </summary>
internal sealed class RequestMessageScheduler(
    IRequestClient<ScheduleDeliveryCommand, ScheduleCreatedEvent> requestClient,
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

        var reply = await requestClient.RequestAsync(command, cancellationToken);
        return reply.ScheduleId;
    }

    public Task CancelInternalAsync(string scheduleId, CancellationToken cancellationToken) =>
        publisher.PublishAsync(new CancelDeliveryCommand { ScheduleId = scheduleId }, cancellationToken);
}
