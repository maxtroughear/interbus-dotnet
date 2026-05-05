using InterBus.Publishing;

namespace InterBus.Scheduling;

public interface IMessageScheduler
{
    Task<Guid> ScheduleInternalAsync<T>(PublishContext<T> context, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken)
        where T : class;

    Task CancelInternalAsync(Guid scheduleId, CancellationToken cancellationToken);
}
