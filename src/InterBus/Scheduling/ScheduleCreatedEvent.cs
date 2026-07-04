namespace InterBus.Scheduling;

/// <summary>
/// Published by <see cref="SchedulingConsumer"/> after a schedule is successfully created by the
/// configured <see cref="IMessageSchedulingProvider"/>. Contains the provider-generated schedule
/// identifier that can be used with <see cref="IScheduler.CancelAsync"/> to cancel the schedule.
/// </summary>
public sealed class ScheduleCreatedEvent
{
    /// <summary>The provider-generated schedule identifier.</summary>
    public required string ScheduleId { get; init; }
}
