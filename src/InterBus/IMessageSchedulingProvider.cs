namespace InterBus;

/// <summary>
/// Schedules messages for deferred delivery. Implemented by transports or dedicated scheduler
/// packages (e.g. <c>InterBus.TickerQ</c>) and registered in DI.
/// <para>
/// Users typically interact with scheduling via the <see cref="IScheduler"/>
/// scheduling methods, which resolve this interface to perform the actual scheduling.
/// </para>
/// </summary>
public interface IMessageSchedulingProvider
{
    /// <summary>
    /// Schedule a serialised payload to be delivered to <paramref name="destination"/> at
    /// <paramref name="scheduledAt"/>. The URI scheme determines the transport and whether
    /// the message is published to a topic (fan-out) or sent directly to a queue
    /// (point-to-point), mirroring the semantics of
    /// <see cref="ITransport.SendAsync"/>.
    /// </summary>
    /// <param name="scheduleId">
    /// The pre-generated identifier for this schedule. Callers generate the ID before calling
    /// this method so that it can be recorded and passed to <see cref="CancelAsync"/> later.
    /// Implementations must use this identifier (e.g. as the resource name in the backing
    /// scheduler) so that <see cref="CancelAsync"/> can locate and remove the schedule.
    /// </param>
    /// <param name="destination">
    /// A URI whose scheme encodes the transport and destination type
    /// (e.g. <c>memorytopic://interbus/my-topic</c> or
    /// <c>memoryqueue://interbus/my-queue</c>). The path is the destination name.
    /// </param>
    /// <param name="payload">The serialised message bytes.</param>
    /// <param name="headers">The message headers.</param>
    /// <param name="scheduledAt">The UTC time at which the message should be delivered.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task ScheduleAsync(Guid scheduleId, Uri destination, byte[] payload, MessageHeaders headers,
        DateTimeOffset scheduledAt, CancellationToken cancellationToken);

    /// <summary>
    /// Cancels a previously scheduled delivery identified by <paramref name="scheduleId"/>.
    /// </summary>
    /// <param name="scheduleId">
    /// The identifier passed to <see cref="ScheduleAsync"/> when the schedule was created.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task CancelAsync(Guid scheduleId, CancellationToken cancellationToken);
}
