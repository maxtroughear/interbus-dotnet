using InterBus.TickerQ.Internal;
using TickerQ.Utilities;
using TickerQ.Utilities.Entities;
using TickerQ.Utilities.Interfaces;
using TickerQ.Utilities.Interfaces.Managers;

namespace InterBus.TickerQ;

/// <summary>
/// <see cref="IMessageSchedulingProvider"/> implementation that uses TickerQ to persist and fire
/// scheduled InterBus messages at their specified delivery time.
/// <para>
/// Register via <c>builder.AddTickerQScheduler()</c> on the <see cref="IInterBusBuilder"/>.
/// </para>
/// </summary>
public sealed class TickerQMessageScheduler : IMessageSchedulingProvider
{
    private readonly ITimeTickerManager<TimeTickerEntity> _manager;
    private readonly ITickerQHostScheduler _hostScheduler;

    /// <summary>
    /// Initializes a new instance of <see cref="TickerQMessageScheduler"/>.
    /// </summary>
    public TickerQMessageScheduler(
        ITimeTickerManager<TimeTickerEntity> manager,
        ITickerQHostScheduler hostScheduler)
    {
        _manager = manager;
        _hostScheduler = hostScheduler;
    }

    /// <inheritdoc/>
    public Task SchedulePublishToTopicAsync(string topicName, byte[] payload, MessageHeaders headers,
        DateTimeOffset scheduledAt, CancellationToken cancellationToken)
    {
        var request = new ScheduledPublishRequest(topicName, payload, headers);
        return ScheduleAsync(TickerQPublishFunction.FunctionName, request, scheduledAt, cancellationToken);
    }

    /// <inheritdoc/>
    public Task ScheduleSendToQueueAsync(string queueName, byte[] payload, MessageHeaders headers,
        DateTimeOffset scheduledAt, CancellationToken cancellationToken)
    {
        var request = new ScheduledSendRequest(queueName, payload, headers);
        return ScheduleAsync(TickerQSendFunction.FunctionName, request, scheduledAt, cancellationToken);
    }

    private async Task ScheduleAsync<TRequest>(string functionName, TRequest request,
        DateTimeOffset scheduledAt, CancellationToken cancellationToken) where TRequest : class
    {
        var ticker = new TimeTickerEntity
        {
            Function = functionName,
            ExecutionTime = scheduledAt.UtcDateTime,
            Request = TickerHelper.CreateTickerRequest(request)
        };

        await _manager.AddAsync(ticker, cancellationToken);

        // Notify the host scheduler so it can wake up if the new job is earlier than the
        // next scheduled poll, giving more precise delivery timing.
        _hostScheduler.RestartIfNeeded(ticker.ExecutionTime);
    }
}

