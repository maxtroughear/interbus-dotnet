using InterBus.Publishing;

namespace InterBus.Scheduling;

/// <summary>
/// Scoped <see cref="IScheduler"/> implementation. Serializes each message and
/// forwards directly to the registered <see cref="IMessageSchedulingProvider"/> for deferred
/// delivery
/// </summary>
internal sealed class DirectMessageScheduler(
    IMessageSchedulingProvider schedulingProvider,
    IMessageSerializer serializer)
    : IMessageScheduler
{
    public async Task<Guid> ScheduleInternalAsync<T>(PublishContext<T> context, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken) where T : class
    {
        var scheduleId = Guid.CreateVersion7();
        var payload = serializer.Serialize(context.Message);
        await schedulingProvider.ScheduleAsync(scheduleId, context.Destination, payload, context.Headers, scheduledAt,
            cancellationToken);
        return scheduleId;
    }

    public Task CancelInternalAsync(Guid scheduleId, CancellationToken cancellationToken) =>
        schedulingProvider.CancelAsync(scheduleId, cancellationToken);
}
