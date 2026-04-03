using InterBus.Middleware;

namespace InterBus.Internal;

/// <summary>
/// <see cref="IMessageSchedulingProvider"/> implementation that uses transport-level delay support
/// to schedule message delivery. The scheduled delivery time is passed through the
/// <see cref="InterBus.Middleware.ScheduleContext"/> and the delay is computed lazily in
/// <see cref="InterBus.Middleware.SchedulePipelineExecutor"/> as <c>scheduledAt - UtcNow</c>
/// immediately before the message is forwarded to the transport via
/// <see cref="TransportSendContext.Delay"/>.
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
        var ctx = new ScheduleContext(destination, payload, headers, scheduledAt);
        return schedulePipeline.ExecuteAsync(ctx, cancellationToken);
    }
}
