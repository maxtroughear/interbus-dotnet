using InterBus.Publisher;
using InterBus.Transport;

namespace InterBus.Scheduler;

/// <summary>
/// <see cref="IScheduler"/> implementation that uses transport-level delay support
/// to schedule message delivery. The delay is computed from <c>scheduledAt - UtcNow</c> and
/// passed to the transport via <see cref="TransportSendContext.Delay"/>.
/// <para>
/// This provider requires no additional dependencies beyond the configured transport. Transports
/// that support deferred visibility (e.g. PostgreSQL <c>visible_at</c>) will honour the delay;
/// transports that do not may deliver the message immediately.
/// </para>
/// </summary>
internal sealed class TransportDelayScheduler(InterBusOptions options, PublishPipelineExecutor publishPipeline)
    : IMessageScheduler
{
    public Task ScheduleInternalAsync<T>(PublishContext<T> context, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken) where T : class
    {
        var delay = scheduledAt - DateTimeOffset.UtcNow;

        // If the scheduled time is already in the past, deliver immediately.
        if (delay < TimeSpan.Zero)
            delay = TimeSpan.Zero;

        context.Delay = delay;

        return publishPipeline.ExecuteAsync(context, cancellationToken: cancellationToken);
    }
}
