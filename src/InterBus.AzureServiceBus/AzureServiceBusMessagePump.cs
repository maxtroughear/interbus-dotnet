using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using InterBus.Middleware;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace InterBus.AzureServiceBus;

/// <summary>
/// Service Activator (Message Pump). A <see cref="BackgroundService"/> that declares the
/// required Azure Service Bus topology (topics, queues, subscriptions) at startup and starts
/// one or more <see cref="ServiceBusProcessor"/> instances per registered InterBus consumer.
/// <para>
/// For each consumer, an Azure Service Bus queue is created to receive direct (point-to-point)
/// sends. For every message type the consumer handles, an Azure Service Bus topic is created
/// (if it does not already exist) and a subscription on that topic is created for the consumer,
/// with <c>ForwardTo</c> set to the consumer's queue. This mirrors the RabbitMQ model of a
/// single queue bound to multiple exchanges.
/// </para>
/// <para>
/// A single <see cref="ServiceBusProcessor"/> per consumer listens on the consumer's queue,
/// receiving both direct sends and messages forwarded from topic subscriptions.
/// </para>
/// <para>
/// Successful dispatch completes the Azure Service Bus message. Consumer failures abandon the
/// message so it is redelivered (up to the entity's <c>MaxDeliveryCount</c>).
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

    /// <summary>
    /// Declares the full Azure Service Bus topology and creates processors before the
    /// background loop starts.
    /// </summary>
    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        if (busOptions.Consumers.Count > 0)
        {
            await DeclareTopologyAsync(cancellationToken);
            await StartProcessorsAsync(cancellationToken);
        }

        await base.StartAsync(cancellationToken);
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (busOptions.Consumers.Count == 0)
        {
            logger.LogInformation(
                "InterBus AzureServiceBus: no consumers registered; message pump idle");
        }

        // Processors run independently; keep the task alive until cancellation.
        return Task.Delay(Timeout.Infinite, stoppingToken)
            .ContinueWith(_ => Task.CompletedTask, TaskContinuationOptions.OnlyOnCanceled)
            .Unwrap();
    }

    /// <summary>
    /// Declares topics, queues, and subscriptions for all registered consumers. Entities that
    /// already exist are left unchanged.
    /// </summary>
    private async Task DeclareTopologyAsync(CancellationToken cancellationToken)
    {
        var admin = clientProvider.AdminClient;

        foreach (var descriptor in busOptions.Consumers)
        {
            var consumerQueueName = AzureServiceBusNaming.SanitizeEntityName(
                options.QueuePrefix + descriptor.QueueName);

            // Create the consumer's queue for direct (point-to-point) sends.
            await EnsureQueueExistsAsync(admin, consumerQueueName, cancellationToken);

            // For each message type, ensure the topic exists and create a forwarding subscription.
            foreach (var messageType in descriptor.MessageTypes)
            {
                var rawTopicName = busOptions.TryGetTopicName(messageType, out var custom)
                    ? custom
                    : RoutingConventions.TopicName(messageType);

                var topicName = AzureServiceBusNaming.SanitizeEntityName(
                    options.TopicPrefix + rawTopicName);

                await EnsureTopicExistsAsync(admin, topicName, cancellationToken);

                var subscriptionName = AzureServiceBusNaming.SanitizeSubscriptionName(
                    options.QueuePrefix + descriptor.QueueName);

                await EnsureForwardingSubscriptionExistsAsync(
                    admin, topicName, subscriptionName, consumerQueueName, cancellationToken);

                logger.LogDebug(
                    "InterBus AzureServiceBus: subscription {SubscriptionName} on topic {TopicName} forwards to queue {QueueName}",
                    subscriptionName,
                    topicName,
                    consumerQueueName);
            }
        }
    }

    private async Task EnsureQueueExistsAsync(
        ServiceBusAdministrationClient admin,
        string queueName,
        CancellationToken cancellationToken)
    {
        try
        {
            await admin.CreateQueueAsync(queueName, cancellationToken);
            logger.LogDebug(
                "InterBus AzureServiceBus: created queue {QueueName}", queueName);
        }
        catch (ServiceBusException ex)
            when (ex.Reason == ServiceBusFailureReason.MessagingEntityAlreadyExists)
        {
            // Queue already exists — nothing to do.
        }
    }

    private async Task EnsureTopicExistsAsync(
        ServiceBusAdministrationClient admin,
        string topicName,
        CancellationToken cancellationToken)
    {
        try
        {
            await admin.CreateTopicAsync(topicName, cancellationToken);
            logger.LogDebug(
                "InterBus AzureServiceBus: created topic {TopicName}", topicName);
        }
        catch (ServiceBusException ex)
            when (ex.Reason == ServiceBusFailureReason.MessagingEntityAlreadyExists)
        {
            // Topic already exists — nothing to do.
        }
    }

    private async Task EnsureForwardingSubscriptionExistsAsync(
        ServiceBusAdministrationClient admin,
        string topicName,
        string subscriptionName,
        string forwardToQueue,
        CancellationToken cancellationToken)
    {
        try
        {
            var subscriptionOptions = new CreateSubscriptionOptions(topicName, subscriptionName)
            {
                ForwardTo = forwardToQueue,
            };
            await admin.CreateSubscriptionAsync(subscriptionOptions, cancellationToken);
            logger.LogDebug(
                "InterBus AzureServiceBus: created subscription {SubscriptionName} on topic {TopicName}",
                subscriptionName,
                topicName);
        }
        catch (ServiceBusException ex)
            when (ex.Reason == ServiceBusFailureReason.MessagingEntityAlreadyExists)
        {
            // Subscription already exists — nothing to do.
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
                options.QueuePrefix + descriptor.QueueName);

            var concurrency = Math.Max(1, descriptor.ConcurrencyCount);
            var processorOptions = new ServiceBusProcessorOptions
            {
                MaxConcurrentCalls = concurrency,
                AutoCompleteMessages = false,
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

    private async Task HandleMessageAsync(
        ConsumerDescriptor descriptor,
        ProcessMessageEventArgs args)
    {
        if (args.CancellationToken.IsCancellationRequested)
        {
            await args.AbandonMessageAsync(args.Message);
            return;
        }

        try
        {
            await DispatchAsync(descriptor, args.Message, args.CancellationToken);
            await args.CompleteMessageAsync(args.Message);
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
                descriptor.QueueName);

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
        ServiceBusReceivedMessage message,
        CancellationToken ct)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var executor = scope.ServiceProvider.GetRequiredService<DispatchPipelineExecutor>();
        var headers = AzureServiceBusTransport.ParseApplicationProperties(message.ApplicationProperties);
        var context = new DispatchContext(descriptor, message.Body.ToArray(), headers);
        await executor.ExecuteAsync(context, ct);
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
}
