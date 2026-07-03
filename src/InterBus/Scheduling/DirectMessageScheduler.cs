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
    public async Task<string?> ScheduleInternalAsync<T>(PublishContext<T> context, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken) where T : class
    {
        var payload = serializer.Serialize(context.Message);
        return await schedulingProvider.ScheduleAsync(context.Destination, payload, context.Headers, scheduledAt,
            cancellationToken);
    }

    public Task CancelInternalAsync(string scheduleId, CancellationToken cancellationToken) =>
        schedulingProvider.CancelAsync(scheduleId, cancellationToken);
}
