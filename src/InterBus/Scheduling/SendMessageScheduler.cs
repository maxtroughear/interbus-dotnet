using InterBus.Publishing;
using InterBus.Requests;

namespace InterBus.Scheduling;

/// <summary>
/// <see cref="IScheduler"/> implementation that asynchronously schedules messages by
/// sending a concrete command message to the bus. The command is consumed by
/// <see cref="SchedulingConsumer"/>, which enlists the registered
/// <see cref="IMessageSchedulingProvider"/> to perform the actual deferred delivery.
/// <para>
/// Returns <c>null</c> from <c>Schedule*Async</c> because the provider-generated schedule ID is
/// not synchronously available. If a <see cref="IReplyEndpointFactory"/> is registered, the
/// scheduler sets a <see cref="InterBusHeaders.ReplyTo"/> header on the command so the
/// <see cref="SchedulingConsumer"/> can reply with a <see cref="ScheduleCreatedEvent"/> and the
/// method returns the provider-generated schedule ID.
/// </para>
/// <para>
/// Register via <c>bus.AddPublishScheduling()</c>. Also register the consumer that processes
/// the commands via <c>bus.AddSchedulingConsumer()</c> - this may be in the same application
/// or in a dedicated consumer service.
/// </para>
/// </summary>
internal sealed class SendMessageScheduler(
    IPublisher publisher,
    IMessageSerializer serializer,
    IReplyEndpointFactory? replyEndpointFactory = null)
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

        if (replyEndpointFactory is not null)
        {
            await using var endpoint = await replyEndpointFactory.CreateAsync(cancellationToken);
            await publisher.SendAsync<SchedulingConsumer, ScheduleDeliveryCommand>(command, ctx =>
                ctx.Headers[InterBusHeaders.ReplyTo] = endpoint.Address.ToString(), cancellationToken);
            var reply = await endpoint.ReceiveAsync<ScheduleCreatedEvent>(cancellationToken);
            return reply.ScheduleId;
        }

        await publisher.SendAsync<SchedulingConsumer, ScheduleDeliveryCommand>(command, cancellationToken);
        return null;
    }

    public Task CancelInternalAsync(string scheduleId, CancellationToken cancellationToken) =>
        publisher.SendAsync<SchedulingConsumer, CancelDeliveryCommand>(
            new CancelDeliveryCommand { ScheduleId = scheduleId }, cancellationToken);
}
