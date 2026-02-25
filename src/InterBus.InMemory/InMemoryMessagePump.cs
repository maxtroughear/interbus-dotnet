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
            queue.Descriptor.QueueName);

        await foreach (var envelope in queue.ReadAllAsync(stoppingToken))
        {
            await DispatchAsync(queue.Descriptor, envelope, stoppingToken);
        }

        _logger.LogDebug(
            "InterBus InMemory: pump for queue {QueueName} stopped",
            queue.Descriptor.QueueName);
    }

    private async Task DispatchAsync(ConsumerDescriptor descriptor, InMemoryEnvelope envelope, CancellationToken ct)
    {
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
