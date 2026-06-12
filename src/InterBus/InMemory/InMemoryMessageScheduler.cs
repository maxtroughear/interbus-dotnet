using System.Collections.Concurrent;
using InterBus.Sending;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace InterBus.InMemory;

public class InMemoryMessageScheduler(IServiceScopeFactory scopeFactory) : BackgroundService, IMessageSchedulingProvider
{
    private readonly SemaphoreSlim _lock = new(1);
    private readonly PriorityQueue<InMemoryScheduledMessage, DateTimeOffset> _scheduledMessages = new();
    private readonly ConcurrentDictionary<Guid, InMemoryScheduledMessage> _scheduledMessagesById = new();

    public async Task ScheduleAsync(Guid scheduleId, Uri destination, byte[] payload, MessageHeaders headers,
        DateTimeOffset scheduledAt,
        CancellationToken cancellationToken)
    {
        await _lock.WaitAsync(cancellationToken);
        try
        {
            var scheduledMessage = new InMemoryScheduledMessage(scheduleId, destination, payload, headers, scheduledAt);
            _scheduledMessages.Enqueue(scheduledMessage, scheduledAt);
            _scheduledMessagesById[scheduleId] = scheduledMessage;
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task CancelAsync(Guid scheduleId, CancellationToken cancellationToken)
    {
        await _lock.WaitAsync(cancellationToken);
        try
        {
            if (_scheduledMessagesById.TryRemove(scheduleId, out var scheduledMessage))
                _scheduledMessages.Remove(scheduledMessage, out _, out _);
        }
        finally
        {
            _lock.Release();
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var delay = false;
        while (!stoppingToken.IsCancellationRequested)
        {
            if (delay)
                await Task.Delay(100, stoppingToken);

            delay = false;

            InMemoryScheduledMessage? message;

            await _lock.WaitAsync(stoppingToken);
            try
            {
                if (_scheduledMessages.TryPeek(out message, out var scheduledAt) &&
                    scheduledAt <= DateTimeOffset.UtcNow)
                {
                    _scheduledMessages.Dequeue();
                    _scheduledMessagesById.TryRemove(message.ScheduleId, out _);
                }
                else
                {
                    delay = true;
                    continue;
                }
            }
            finally
            {
                _lock.Release();
            }

            using var scope = scopeFactory.CreateScope();
            var sendPipeline = scope.ServiceProvider.GetRequiredService<SendPipelineExecutor>();
            var scheduleContext = new SendContext(message.Destination, message.Payload, message.Headers, delay: null);
            await sendPipeline.ExecuteAsync(scheduleContext, cancellationToken: stoppingToken);
        }
    }
}
