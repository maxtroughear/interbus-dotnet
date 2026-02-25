using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace InterBus.InMemory;

/// <summary>
/// Service Activator (Message Pump). A <see cref="BackgroundService"/> that starts one reader task per
/// registered queue and dispatches messages to consumers resolved from a fresh DI scope per message.
/// </summary>
internal sealed class InMemoryMessagePump : BackgroundService
{
    private readonly InMemoryBroker _broker;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<InMemoryMessagePump> _logger;

    public InMemoryMessagePump(
        InMemoryBroker broker,
        IServiceScopeFactory scopeFactory,
        ILogger<InMemoryMessagePump> logger)
    {
        _broker = broker;
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var queues = _broker.GetAllQueues();
        if (queues.Count == 0)
        {
            _logger.LogInformation("InterBus InMemory: no consumers registered; message pump idle");
            return Task.CompletedTask;
        }

        // Start one reader per queue; all run concurrently.
        var tasks = queues.Select(q => ProcessQueueAsync(q, stoppingToken));
        return Task.WhenAll(tasks);
    }

    private async Task ProcessQueueAsync(InMemoryQueue queue, CancellationToken stoppingToken)
    {
        _logger.LogDebug(
            "InterBus InMemory: starting pump for queue {QueueName}",
            queue.QueueName);

        await foreach (var envelope in queue.ReadAllAsync(stoppingToken))
        {
            await DispatchAsync(queue, envelope, stoppingToken);
        }

        _logger.LogDebug(
            "InterBus InMemory: pump for queue {QueueName} stopped",
            queue.QueueName);
    }

    private async Task DispatchAsync(InMemoryQueue queue, InMemoryEnvelope envelope, CancellationToken ct)
    {
        var descriptor = queue.FindHandler(envelope.TopicName);
        if (descriptor is null)
        {
            _logger.LogWarning(
                "InterBus InMemory: no handler registered on queue {QueueName} for topic {TopicName}",
                queue.QueueName,
                envelope.TopicName);
            return;
        }

        await using var scope = _scopeFactory.CreateAsyncScope();
        try
        {
            await descriptor.Dispatch(scope.ServiceProvider, envelope.Payload, envelope.Headers, ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(
                ex,
                "InterBus InMemory: unhandled exception in consumer {ConsumerType} for message {MessageType}",
                descriptor.ConsumerType.Name,
                descriptor.MessageType.Name);
        }
    }
}
