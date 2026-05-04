using InterBus.Publishing;
using InterBus.Transporting;

namespace InterBus.Scheduling;

/// <summary>
/// <see cref="IScheduler"/> implementation that uses transport-level delay support
/// to schedule message delivery. The delay is computed from <c>scheduledAt - UtcNow</c> and
/// passed to the transport via <see cref="TransportSendContext.Delay"/>.
/// <para>
/// This provider requires no additional dependencies beyond the configured transport. Transports
/// that support deferred visibility (e.g. PostgreSQL <c>visible_at</c>) will honour the delay;
/// transports that do not may deliver the message immediately.
/// </para>
/// <para>
/// Cancellation is not supported by this scheduler. <see cref="CancelInternalAsync"/> always
/// throws <see cref="NotSupportedException"/>.
/// </para>
/// </summary>
internal sealed class TransportDelayScheduler(PublishPipelineExecutor publishPipeline)
    : IMessageScheduler
{
    public async Task<Guid> ScheduleInternalAsync<T>(PublishContext<T> context, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken) where T : class
    {
        var delay = scheduledAt - DateTimeOffset.UtcNow;

        // If the scheduled time is already in the past, deliver immediately.
        if (delay < TimeSpan.Zero)
            delay = TimeSpan.Zero;

        context.Delay = delay;

        await publishPipeline.ExecuteAsync(context, cancellationToken: cancellationToken);
        return Guid.CreateVersion7();
    }

    public Task CancelInternalAsync(Guid scheduleId, CancellationToken cancellationToken) =>
        throw new NotSupportedException(
            "The transport-delay scheduler does not support cancellation. " +
            "Use a scheduler that supports cancellation (e.g. TickerQ or EventBridge).");
}
