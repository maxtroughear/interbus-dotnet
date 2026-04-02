namespace InterBus.Internal;

/// <summary>
/// <see cref="IMessageSchedulingProvider"/> implementation that converts a scheduled delivery
/// time into a transport-level delay and forwards the message to the appropriate transport.
/// <para>
/// The delay is calculated as <c>scheduledAt - DateTimeOffset.UtcNow</c>, clamped to a minimum
/// of <see cref="TimeSpan.Zero"/>. The <see cref="TransportSendContext.Delay"/> is then passed
/// to <see cref="IInterBusTransport.SendAsync"/>, which must support deferred delivery
/// (e.g. <c>InMemoryBroker</c> uses <c>Task.Delay</c>; <c>PostgreSqlTransport</c> uses a
/// database <c>visible_at</c> column).
/// </para>
/// <para>
/// Register via <c>builder.AddTransportDelayScheduler()</c>. This provider introduces no
/// additional dependencies beyond the configured transport.
/// </para>
/// </summary>
internal sealed class TransportDelaySchedulingProvider(IInterBusTransportFactory transportFactory)
    : IMessageSchedulingProvider
{
    /// <inheritdoc/>
    public Task ScheduleAsync(Uri destination, byte[] payload, MessageHeaders headers,
        DateTimeOffset scheduledAt, CancellationToken cancellationToken)
    {
        var delay = scheduledAt - DateTimeOffset.UtcNow;
        if (delay < TimeSpan.Zero)
            delay = TimeSpan.Zero;

        var transport = transportFactory.Resolve(destination);
        var context = new TransportSendContext(destination, payload, headers, delay);
        return transport.SendAsync(context, cancellationToken);
    }
}
