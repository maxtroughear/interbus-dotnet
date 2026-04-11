using InterBus.Middleware;

namespace InterBus.Internal;

/// <summary>
/// Scoped <see cref="IMessageScheduler"/> implementation. Serializes each message and
/// forwards directly to the registered <see cref="IMessageSchedulingProvider"/> for deferred
/// delivery. Any cross-cutting delivery concerns (e.g. the Claim Check pattern) are handled
/// by the <see cref="IScheduleMiddleware"/> pipeline that runs on the
/// provider side via <see cref="SchedulePipelineExecutor"/> when the job fires.
/// </summary>
internal sealed class MessageScheduler(
    IMessageSchedulingProvider schedulingProvider,
    InterBusOptions options,
    IMessageSerializer serializer)
    : MessageSchedulerBase(options, serializer)
{
    protected override Task ScheduleInternalAsync<T>(PublishContext<T> context, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken)
    {
        var payload = Serializer.Serialize(context.Message);
        return schedulingProvider.ScheduleAsync(context.Destination, payload, context.Headers, scheduledAt,
            cancellationToken);
    }
}
