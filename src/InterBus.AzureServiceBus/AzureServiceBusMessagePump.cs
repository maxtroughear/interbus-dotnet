using Azure.Messaging.ServiceBus;
using InterBus.Consuming;
using InterBus.Dispatching;
using InterBus.MessagePumps;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace InterBus.AzureServiceBus;

internal sealed class AzureServiceBusMessagePump(
    ConsumerDescriptor descriptor, AzureServiceBusClientProvider clientProvider,
    AzureServiceBusTransportOptions options, ILogger<AzureServiceBusMessagePump> logger) : IManagedSource
{
    private ServiceBusProcessor? _processor;
    private ServiceBusSessionProcessor? _sessions;

    public Task InitializeAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (descriptor.PumpCount != 1)
            throw new InvalidOperationException("Azure Service Bus supports PumpCount = 1 only");
        var queue = AzureServiceBusNaming.SanitizeEntityName(options.QueuePrefix + descriptor.QueueName);
        if (descriptor.IsFifo)
            _sessions = clientProvider.CreateQueueSessionProcessor(queue, new ServiceBusSessionProcessorOptions
            {
                MaxConcurrentSessions = descriptor.ConcurrencyCount,
                MaxConcurrentCallsPerSession = 1,
                SessionIdleTimeout = TimeSpan.FromSeconds(2),
                AutoCompleteMessages = false,
                ReceiveMode = ServiceBusReceiveMode.PeekLock,
                MaxAutoLockRenewalDuration = TimeSpan.FromHours(12)
            });
        else
            _processor = clientProvider.CreateQueueProcessor(queue, new ServiceBusProcessorOptions
            {
                MaxConcurrentCalls = descriptor.ConcurrencyCount,
                AutoCompleteMessages = false,
                ReceiveMode = ServiceBusReceiveMode.PeekLock,
                MaxAutoLockRenewalDuration = TimeSpan.FromHours(12)
            });
        return Task.CompletedTask;
    }

    public async Task RunAsync(Func<IDelivery, CancellationToken, Task> process, CancellationToken intakeToken,
        CancellationToken processingToken)
    {
        if (_processor is not null)
        {
            _processor.ProcessMessageAsync += async args =>
            {
                using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(args.CancellationToken, processingToken);
                await process(new ServiceBusDelivery(args.Message, async (result, ct) =>
                {
                    switch (result.Kind)
                    {
                        case DispatchResultKind.Success: await args.CompleteMessageAsync(args.Message, ct); break;
                        case DispatchResultKind.Release: await args.AbandonMessageAsync(args.Message, cancellationToken: ct); break;
                        case DispatchResultKind.DeadLetter:
                            await args.DeadLetterMessageAsync(args.Message, Properties(result), result.DeadLetterReason,
                                result.DeadLetterError, ct); break;
                        default: throw new ArgumentOutOfRangeException(nameof(result));
                    }
                }), cancellation.Token);
            };
            _processor.ProcessErrorAsync += ReportErrorAsync;
            try
            {
                await _processor.StartProcessingAsync(intakeToken);
                await Task.Delay(Timeout.Infinite, intakeToken);
            }
            catch (OperationCanceledException) when (intakeToken.IsCancellationRequested) { }
            finally { await _processor.StopProcessingAsync(CancellationToken.None); }
        }
        else if (_sessions is not null)
        {
            _sessions.ProcessMessageAsync += async args =>
            {
                using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(args.CancellationToken, processingToken);
                await process(new ServiceBusDelivery(args.Message, async (result, ct) =>
                {
                    switch (result.Kind)
                    {
                        case DispatchResultKind.Success: await args.CompleteMessageAsync(args.Message, ct); break;
                        case DispatchResultKind.Release: await args.AbandonMessageAsync(args.Message, cancellationToken: ct); break;
                        case DispatchResultKind.DeadLetter:
                            await args.DeadLetterMessageAsync(args.Message, Properties(result), result.DeadLetterReason,
                                result.DeadLetterError, ct); break;
                        default: throw new ArgumentOutOfRangeException(nameof(result));
                    }
                }), cancellation.Token);
            };
            _sessions.ProcessErrorAsync += ReportErrorAsync;
            try
            {
                await _sessions.StartProcessingAsync(intakeToken);
                await Task.Delay(Timeout.Infinite, intakeToken);
            }
            catch (OperationCanceledException) when (intakeToken.IsCancellationRequested) { }
            finally { await _sessions.StopProcessingAsync(CancellationToken.None); }
        }
    }

    private Task ReportErrorAsync(ProcessErrorEventArgs args)
    {
        logger.LogError(args.Exception, "InterBus Azure Service Bus: processor error for {Consumer}; source={Source}",
            descriptor.ConsumerType, args.ErrorSource);
        return Task.CompletedTask;
    }

    private static Dictionary<string, object> Properties(DispatchResult result) => new()
    {
        [InterBusHeaders.DeadLetterReason] = result.DeadLetterReason,
        [InterBusHeaders.DeadLetterError] = result.DeadLetterError,
        [InterBusHeaders.DeadLetterTime] = result.DeadLetteredAtUtc!.Value.ToString("O")
    };

    public async ValueTask DisposeAsync()
    {
        if (_processor is not null) await _processor.DisposeAsync();
        if (_sessions is not null) await _sessions.DisposeAsync();
    }

    private sealed class ServiceBusDelivery : IDelivery
    {
        private readonly Func<DispatchResult, CancellationToken, Task> _settle;
        public ReadOnlyMemory<byte> Payload { get; }
        public MessageHeaders Headers { get; } = new();
        public ServiceBusDelivery(ServiceBusReceivedMessage message, Func<DispatchResult, CancellationToken, Task> settle)
        {
            Payload = message.Body.ToArray();
            _settle = settle;
            foreach (var (key, value) in message.ApplicationProperties)
                if (value is not null) Headers[key] = value.ToString()!;
        }
        public async Task<bool> SettleAsync(DispatchResult result, CancellationToken cancellationToken)
        {
            try { await _settle(result, cancellationToken); return true; }
            catch (ServiceBusException error) when (error.Reason is ServiceBusFailureReason.MessageLockLost or ServiceBusFailureReason.SessionLockLost)
            { return false; }
        }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}

internal sealed class AzureServiceBusSourceFactory(IServiceProvider provider) : IConsumerSourceFactory
{
    public IConsumerSource Create(ConsumerDescriptor descriptor) =>
        ActivatorUtilities.CreateInstance<AzureServiceBusMessagePump>(provider, descriptor);
}
