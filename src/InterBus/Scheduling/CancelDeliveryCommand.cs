namespace InterBus.Scheduling;

/// <summary>
/// Internal command message published to the bus to request that a previously scheduled
/// delivery be cancelled via the configured <see cref="IMessageSchedulingProvider"/>
/// </summary>
internal sealed class CancelDeliveryCommand
{
    /// <summary>The identifier of the schedule to cancel, as returned by <see cref="ScheduleDeliveryCommand.ScheduleId"/></summary>
    public required Guid ScheduleId { get; init; }
}
