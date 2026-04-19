using InterBus.Publisher;

namespace InterBus.Scheduler;

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
    public Task ScheduleInternalAsync<T>(PublishContext<T> context, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken) where T : class
    {
        var payload = serializer.Serialize(context.Message);
        return schedulingProvider.ScheduleAsync(context.Destination, payload, context.Headers, scheduledAt,
            cancellationToken);
    }
}
