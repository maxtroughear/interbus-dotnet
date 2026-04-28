using InterBus.Publishing;

namespace InterBus.Scheduling;

public interface IMessageScheduler
{
    Task ScheduleInternalAsync<T>(PublishContext<T> context, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken)
        where T : class;
}
