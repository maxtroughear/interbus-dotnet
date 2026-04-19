using InterBus.Publisher;

namespace InterBus.Scheduler;

public interface IMessageScheduler
{
    Task ScheduleInternalAsync<T>(PublishContext<T> context, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken)
        where T : class;
}
