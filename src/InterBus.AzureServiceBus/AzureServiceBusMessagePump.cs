using Azure.Messaging.ServiceBus;
using InterBus.Consuming;
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
                ReceiveMode = ServiceBusReceiveMode.PeekLock,
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

        try
        {
            var result = await DispatchAsync(descriptor, args.Message, args.CancellationToken);
            switch (result.Kind)
            {
                case DispatchResultKind.Success:
                    await args.CompleteMessageAsync(args.Message);
                    break;
                case DispatchResultKind.DeadLetter:
                    await args.DeadLetterMessageAsync(
                        args.Message,
                        deadLetterReason: result.DeadLetterReason,
                        deadLetterErrorDescription: result.DeadLetterError,
                        propertiesToModify: new Dictionary<string, object>
                        {
                            [InterBusHeaders.DeadLetterReason] = result.DeadLetterReason!,
                            [InterBusHeaders.DeadLetterError] = result.DeadLetterError!,
                            [InterBusHeaders.DeadLetteredAt] = result.DeadLetteredAtUtc!.Value.ToString("O"),
                        },
                        cancellationToken: CancellationToken.None);

                    logger.LogWarning(
                        result.Exception,
                        "InterBus AzureServiceBus: consumer {ConsumerType} failed for queue {QueueName}; message moved to dead-letter queue",
                        descriptor.ConsumerType.Name,
                        descriptor.Queue);
                    break;
                case DispatchResultKind.Release:
                    logger.LogWarning(
                        result.Exception,
                        "InterBus AzureServiceBus: consumer {ConsumerType} failed for queue {QueueName}; abandoning message for redelivery",
                        descriptor.ConsumerType.Name,
                        descriptor.Queue);

                    await args.AbandonMessageAsync(args.Message, cancellationToken: CancellationToken.None);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
        catch (OperationCanceledException) when (args.CancellationToken.IsCancellationRequested)
        {
            await args.AbandonMessageAsync(args.Message, cancellationToken: CancellationToken.None);
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

    private async Task<DispatchResult> DispatchAsync(
        ConsumerDescriptor descriptor,
        ServiceBusReceivedMessage message,
        CancellationToken ct)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var executor = scope.ServiceProvider.GetRequiredService<DispatchPipelineExecutor>();
        var headers = ParseApplicationProperties(message.ApplicationProperties);
        var context = new DispatchContext(descriptor, message.Body.ToArray(), headers);
        await executor.ExecuteAsync(context, ct);
        return context.Result;
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
