using InterBus.Middleware;

namespace InterBus.Internal;

/// <summary>
/// <see cref="IMessageSchedulingProvider"/> implementation that converts a scheduled delivery
/// time into a transport-level delay and runs the message through the
/// <see cref="SchedulePipelineExecutor"/> for delivery.
/// <para>
/// The delay is calculated as <c>scheduledAt - DateTimeOffset.UtcNow</c>, clamped to a minimum
/// of <see cref="TimeSpan.Zero"/>. The delay is attached to the <see cref="ScheduleContext"/>
/// and forwarded to the transport as <see cref="TransportSendContext.Delay"/> by the terminal
/// step of the schedule pipeline. Transports that support deferred visibility
/// (e.g. <c>InMemoryBroker</c> via <c>Task.Delay</c>; <c>PostgreSqlTransport</c> via a
/// database <c>visible_at</c> column) will honour this value.
/// </para>
/// <para>
/// Register via <c>builder.AddTransportDelayScheduler()</c>. This provider introduces no
/// additional dependencies beyond the configured transport.
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
        if (delay < TimeSpan.Zero)
            delay = TimeSpan.Zero;

        var context = new ScheduleContext(destination, payload, headers) { Delay = delay };
        return schedulePipeline.ExecuteAsync(context, cancellationToken);
    }
}
