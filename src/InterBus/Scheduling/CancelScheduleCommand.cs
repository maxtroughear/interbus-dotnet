namespace InterBus.Scheduling;

/// <summary>
/// Internal command message published to the bus to request that a previously scheduled
/// delivery be cancelled via the configured <see cref="IMessageSchedulingProvider"/>.
/// Consumed by <see cref="CancelSchedulingConsumer"/>.
/// </summary>
internal sealed class CancelScheduleCommand
{
    /// <summary>The identifier of the schedule to cancel, as returned by <see cref="ScheduleDeliveryCommand.ScheduleId"/>.</summary>
    public Guid ScheduleId { get; init; }
}
