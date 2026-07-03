using InterBus.Publishing;

namespace InterBus.Scheduling;

public interface IMessageScheduler
{
    Task<string?> ScheduleInternalAsync<T>(PublishContext<T> context, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken)
        where T : class;

    Task CancelInternalAsync(string scheduleId, CancellationToken cancellationToken);
}
