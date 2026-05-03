using InterBus.Consuming;
using InterBus.Dispatching;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace InterBus.InMemory;

/// <summary>
/// Service Activator (Message Pump). A <see cref="BackgroundService"/> that starts one reader task per
/// registered queue and dispatches messages to consumers resolved from a fresh DI scope per message.
/// Message dispatch runs through the <see cref="ConsumePipelineExecutor"/> so that all registered
/// consume middleware (including the default exception-logging step) is applied to every message.
/// </summary>
internal sealed class InMemoryMessagePump(
    InMemoryBroker broker,
    IServiceScopeFactory scopeFactory,
    ILogger<InMemoryMessagePump> logger)
    : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var queues = broker.GetAllQueues();
        if (queues.Count == 0)
        {
            logger.LogInformation("InterBus InMemory: no consumers registered; message pump idle");
            return Task.CompletedTask;
        }

        // Start one reader per queue; all run concurrently.
        var tasks = queues.Select(q => ProcessQueueAsync(q, stoppingToken));
        return Task.WhenAll(tasks);
    }

    private async Task ProcessQueueAsync(InMemoryQueue queue, CancellationToken stoppingToken)
    {
        logger.LogDebug(
            "InterBus InMemory: starting pump for queue {QueueName}",
            queue.Descriptor.QueueName);

        await foreach (var envelope in queue.ReadAllAsync(stoppingToken))
        {
            try
            {
                await DispatchAsync(queue.Descriptor, envelope, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                logger.LogWarning(
                    ex,
                    "InterBus InMemory: consumer {ConsumerType} failed for queue {QueueName}; releasing message back to queue for redelivery",
                    queue.Descriptor.ConsumerType.Name,
                    queue.Descriptor.QueueName);

                // In-memory transport simulates broker behaviour by re-enqueueing the same
                // envelope when processing fails (no ACK semantics).
                // Yield to the thread pool scheduler before re-enqueuing so that rapid failure
                // loops do not starve other consumer pumps running on the same thread pool.
                await Task.Yield();
                await queue.WriteAsync(envelope, stoppingToken);
            }
        }

        logger.LogDebug(
            "InterBus InMemory: pump for queue {QueueName} stopped",
            queue.Descriptor.QueueName);
    }

    private async Task DispatchAsync(ConsumerDescriptor descriptor, InMemoryEnvelope envelope, CancellationToken ct)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var executor = scope.ServiceProvider.GetRequiredService<DispatchPipelineExecutor>();
        var context = new DispatchContext(descriptor, envelope.Payload, envelope.Headers);
        await executor.ExecuteAsync(context, ct);
    }
}
