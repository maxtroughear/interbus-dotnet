using InterBus.Middleware;

namespace InterBus.Internal;

/// <summary>
/// <see cref="IMessageSchedulingProvider"/> implementation that uses transport-level delay support
/// to schedule message delivery. The delay is computed from <c>scheduledAt - UtcNow</c> and
/// passed to the transport via <see cref="TransportSendContext.Delay"/>.
/// <para>
/// This provider requires no additional dependencies beyond the configured transport. Transports
/// that support deferred visibility (e.g. PostgreSQL <c>visible_at</c>) will honour the delay;
/// transports that do not may deliver the message immediately.
/// </para>
/// <para>
/// Register via <c>builder.AddTransportDelayScheduler()</c> on the <see cref="IInterBusBuilder"/>.
/// </para>
/// </summary>
internal sealed class TransportDelaySchedulingProvider(SchedulePipelineExecutor schedulePipeline)
    : IMessageSchedulingProvider
{
    /// <inheritdoc/>
    public Task ScheduleAsync(Uri destination, byte[] payload, MessageHeaders headers,
        DateTimeOffset scheduledAt, CancellationToken cancellationToken)
    {
        var delay = scheduledAt - DateTimeOffset.UtcNow;

        // If the scheduled time is already in the past, deliver immediately.
        if (delay < TimeSpan.Zero)
            delay = TimeSpan.Zero;

        var ctx = new ScheduleContext(destination, payload, headers, delay);
        return schedulePipeline.ExecuteAsync(ctx, cancellationToken);
    }
}
