using Azure.Messaging.ServiceBus;
using InterBus.Consuming;
using InterBus.DeadLettering;
using InterBus.Dispatching;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace InterBus.AzureServiceBus;

/// <summary>
/// Service Activator (Message Pump). A <see cref="BackgroundService"/> that starts one or more
/// <see cref="ServiceBusProcessor"/> instances per registered InterBus consumer.
/// <para>Topology is created or verified during bus startup before this pump starts.</para>
/// <para>
/// A single <see cref="ServiceBusProcessor"/> per consumer listens on the consumer's queue,
/// receiving both direct sends and messages forwarded from topic subscriptions.
/// </para>
/// <para>
/// Successful dispatch completes the Azure Service Bus message. Unhandled consumer failures are
/// dead-lettered with diagnostic details; other transient failures are abandoned for redelivery.
/// </para>
/// </summary>
internal sealed class AzureServiceBusMessagePump(
    AzureServiceBusClientProvider clientProvider,
    AzureServiceBusTransportOptions options,
    InterBusOptions busOptions,
    IServiceScopeFactory scopeFactory,
    ILogger<AzureServiceBusMessagePump> logger)
    : BackgroundService
{
    private readonly List<ServiceBusProcessor> _processors = [];

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        if (busOptions.Consumers.Count > 0)
            await StartProcessorsAsync(cancellationToken);

        await base.StartAsync(cancellationToken);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);

        foreach (var processor in _processors)
        {
            await processor.StopProcessingAsync(CancellationToken.None);
            await processor.DisposeAsync();
        }

        _processors.Clear();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (busOptions.Consumers.Count == 0)
        {
            logger.LogInformation(
                "InterBus AzureServiceBus: no consumers registered; message pump idle");
            return;
        }

        // Processors run independently via callbacks; keep the hosted service alive until
        // the application shuts down.
        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            // Expected on shutdown.
        }
    }

    /// <summary>
    /// Creates and starts one <see cref="ServiceBusProcessor"/> per consumer, each listening
    /// on the consumer's dedicated queue.
    /// </summary>
    private async Task StartProcessorsAsync(CancellationToken cancellationToken)
    {
        foreach (var descriptor in busOptions.Consumers)
        {
            var consumerQueueName = AzureServiceBusNaming.SanitizeEntityName(
                options.QueuePrefix + descriptor.Queue);

            var concurrency = Math.Max(1, descriptor.ConcurrencyCount);
            var processorOptions = new ServiceBusProcessorOptions
            {
                MaxConcurrentCalls = concurrency,
                AutoCompleteMessages = false,
                MaxAutoLockRenewalDuration = TimeSpan.FromHours(12), // TODO: configurable by consumer
            };

            var processor = clientProvider.CreateQueueProcessor(consumerQueueName, processorOptions);

            processor.ProcessMessageAsync += args => HandleMessageAsync(descriptor, args);
            processor.ProcessErrorAsync += args => HandleErrorAsync(descriptor, args);

            await processor.StartProcessingAsync(cancellationToken);
            _processors.Add(processor);

            logger.LogDebug(
                "InterBus AzureServiceBus: started processor for consumer {ConsumerType} on queue {QueueName}",
                descriptor.ConsumerType.Name,
                consumerQueueName);
        }
    }

    private async Task HandleMessageAsync(ConsumerDescriptor descriptor, ProcessMessageEventArgs args)
    {
        if (args.CancellationToken.IsCancellationRequested)
        {
            await args.AbandonMessageAsync(args.Message);
            return;
        }

        var headers = ParseApplicationProperties(args.Message.ApplicationProperties);
        try
        {
            await DispatchAsync(descriptor, args.Message.Body.ToArray(), headers, args.CancellationToken);
            await args.CompleteMessageAsync(args.Message);
        }
        catch (OperationCanceledException) when (args.CancellationToken.IsCancellationRequested)
        {
            await args.AbandonMessageAsync(args.Message, cancellationToken: CancellationToken.None);
        }
        catch (DeadLetterException ex)
        {
            await args.DeadLetterMessageAsync(
                args.Message,
                deadLetterReason: ex.Reason,
                deadLetterErrorDescription: ex.Error,
                propertiesToModify: new Dictionary<string, object>
                {
                    [InterBusHeaders.DeadLetterReason] = ex.Reason,
                    [InterBusHeaders.DeadLetterError] = ex.Error,
                    [InterBusHeaders.DeadLetteredAt] = ex.DeadLetteredAtUtc.ToString("O"),
                },
                cancellationToken: CancellationToken.None);

            logger.LogWarning(
                ex,
                "InterBus AzureServiceBus: consumer {ConsumerType} failed for queue {QueueName}; message moved to dead-letter queue",
                descriptor.ConsumerType.Name,
                descriptor.Queue);
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "InterBus AzureServiceBus: consumer {ConsumerType} failed for queue {QueueName}; abandoning message for redelivery",
                descriptor.ConsumerType.Name,
                descriptor.Queue);

            await args.AbandonMessageAsync(args.Message, cancellationToken: CancellationToken.None);
        }
    }

    private Task HandleErrorAsync(ConsumerDescriptor descriptor, ProcessErrorEventArgs args)
    {
        logger.LogError(
            args.Exception,
            "InterBus AzureServiceBus: processor error for consumer {ConsumerType}; source={ErrorSource}",
            descriptor.ConsumerType.Name,
            args.ErrorSource);

        return Task.CompletedTask;
    }

    private async Task DispatchAsync(
        ConsumerDescriptor descriptor,
        byte[] body,
        MessageHeaders headers,
        CancellationToken ct)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var executor = scope.ServiceProvider.GetRequiredService<DispatchPipelineExecutor>();
        var context = new DispatchContext(descriptor, body, headers);
        await executor.ExecuteAsync(context, ct);
    }

    private static MessageHeaders ParseApplicationProperties(
        IReadOnlyDictionary<string, object?> applicationProperties)
    {
        var dict = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var kv in applicationProperties)
        {
            var value = kv.Value switch
            {
                string str => str,
                _ => kv.Value?.ToString(),
            };
            if (value is not null)
                dict[kv.Key] = value;
        }

        return new MessageHeaders(dict);
    }
}
