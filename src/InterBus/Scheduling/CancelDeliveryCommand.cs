namespace InterBus.Scheduling;

/// <summary>
/// Internal command message published to the bus to request that a previously scheduled
/// delivery be cancelled via the configured <see cref="IMessageSchedulingProvider"/>
/// </summary>
internal sealed class CancelDeliveryCommand
{
    /// <summary>The provider-generated schedule identifier to cancel, as returned by <see cref="IMessageSchedulingProvider.ScheduleAsync"/>.</summary>
    public required string ScheduleId { get; init; }
}
