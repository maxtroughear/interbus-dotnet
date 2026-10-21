using Azure.Messaging.ServiceBus;
using InterBus.Consuming;
using InterBus.Dispatching;
using InterBus.MessagePumps;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.AzureServiceBus;

internal sealed class AzureServiceBusMessagePump(
    ConsumerDescriptor descriptor, AzureServiceBusClientProvider clientProvider,
    AzureServiceBusTransportOptions options, ILogger<AzureServiceBusMessagePump> logger) : ICallbackSource
{
    private readonly TaskCompletionSource<Exception> _failure = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public Task<Exception> Failure => _failure.Task;
    private readonly TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public Task Ready => _ready.Task;
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

    public async Task StartAsync(DeliveryHandler process, CancellationToken processingToken, CancellationToken operationToken)
    {
        async Task ProcessAsync(IDelivery delivery, CancellationToken token)
        {
            try { await process(delivery, token); }
            catch (Exception error) { _failure.TrySetResult(error); throw; }
        }
        if (_processor is not null)
        {
            _processor.ProcessMessageAsync += async args =>
            {
                using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(processingToken);
                using var callbackCancellation = args.CancellationToken.Register(() =>
                {
                    // SDK stop cancels its callback token before accepted work has drained
                    if (Volatile.Read(ref _pausing) == 0) cancellation.Cancel();
                });
                await ProcessAsync(new ServiceBusDelivery(args.Message, async (result, ct) =>
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
            await _processor.StartProcessingAsync(operationToken);
            _ready.TrySetResult();
        }
        else if (_sessions is not null)
        {
            _sessions.ProcessMessageAsync += async args =>
            {
                using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(processingToken);
                using var callbackCancellation = args.CancellationToken.Register(() =>
                {
                    // SDK stop cancels its callback token before accepted work has drained
                    if (Volatile.Read(ref _pausing) == 0) cancellation.Cancel();
                });
                await ProcessAsync(new ServiceBusDelivery(args.Message, async (result, ct) =>
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
            await _sessions.StartProcessingAsync(operationToken);
            _ready.TrySetResult();
        }
    }

    private int _pausing;
    public async Task PauseAsync(CancellationToken operationToken)
    {
        Volatile.Write(ref _pausing, 1);
        if (_processor is not null) await _processor.StopProcessingAsync(operationToken);
        if (_sessions is not null) await _sessions.StopProcessingAsync(operationToken);
    }

    public async Task ResumeAsync(CancellationToken operationToken)
    {
        Volatile.Write(ref _pausing, 0);
        try
        {
            if (_processor is not null) await _processor.StartProcessingAsync(operationToken);
            if (_sessions is not null) await _sessions.StartProcessingAsync(operationToken);
        }
        catch (Exception error) { _failure.TrySetResult(error); throw; }
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
