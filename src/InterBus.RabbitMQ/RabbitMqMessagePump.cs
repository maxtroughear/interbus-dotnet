using InterBus.Consuming;
using InterBus.Dispatching;
using InterBus.MessagePumps;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace InterBus.RabbitMQ;

internal sealed class RabbitMqMessagePump(
    ConsumerDescriptor descriptor, RabbitMqConnectionProvider connectionProvider,
    RabbitMqTransportOptions options, ILogger<RabbitMqMessagePump> logger) : IManagedSource
{
    private readonly ConsumerDescriptor _descriptor = descriptor;
    private IConnection? _connection;
    private IChannel? _channel;
    private int _generation;
    private readonly SemaphoreSlim _dispatchGate = new(descriptor.ConcurrencyCount);
    private readonly TaskCompletionSource<Exception> _failure = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public Task<Exception> Failure => _failure.Task;
    private readonly TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public Task Ready => _ready.Task;
    private readonly object _gate = new();
    private int _active;
    private TaskCompletionSource _idle = Completed();

    private static TaskCompletionSource Completed()
    {
        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        completion.SetResult();
        return completion;
    }

    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        if (_descriptor.PumpCount != 1) throw new InvalidOperationException("RabbitMQ supports PumpCount = 1 only");
        if (_descriptor.ConcurrencyCount > ushort.MaxValue) throw new InvalidOperationException("RabbitMQ concurrency exceeds 65535");
        _connection = await ConnectWithRetryAsync(cancellationToken);
        _connection.ConnectionShutdownAsync += (_, _) =>
        { Interlocked.Increment(ref _generation); return Task.CompletedTask; };
        var concurrency = (ushort)_descriptor.ConcurrencyCount;
        _channel = await _connection.CreateChannelAsync(new CreateChannelOptions(
            publisherConfirmationsEnabled: false, publisherConfirmationTrackingEnabled: false,
            consumerDispatchConcurrency: concurrency), cancellationToken);
        await _channel.BasicQosAsync(0, concurrency, false, cancellationToken);
        _channel.ChannelShutdownAsync += (_, reason) =>
        {
            if (reason.Initiator != ShutdownInitiator.Application && _connection.IsOpen)
                _failure.TrySetResult(new InvalidOperationException($"RabbitMQ consumer channel closed: {reason.ReplyText}"));
            return Task.CompletedTask;
        };
    }

    public async Task RunAsync(Func<IDelivery, CancellationToken, Task> process, CancellationToken intakeToken,
        CancellationToken processingToken)
    {
        var channel = _channel!;
        var consumer = new AsyncEventingBasicConsumer(channel);
        var unregistered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        consumer.UnregisteredAsync += (_, _) => { unregistered.TrySetResult(); return Task.CompletedTask; };
        consumer.ReceivedAsync += async (_, args) =>
        {
            lock (_gate) { if (_active++ == 0) _idle = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously); }
            RabbitDelivery? delivery = null;
            var transferred = false;
            try
            {
                delivery = new RabbitDelivery(this, args);
                await _dispatchGate.WaitAsync(processingToken);
                try { transferred = true; await process(delivery, processingToken); }
                finally { _dispatchGate.Release(); }
            }
            catch (OperationCanceledException) when (processingToken.IsCancellationRequested) { }
            catch (Exception error) { _failure.TrySetResult(error); throw; }
            finally
            {
                try { if (!transferred && delivery is not null) await delivery.DisposeAsync(); }
                finally { lock (_gate) { if (--_active == 0) _idle.TrySetResult(); } }
            }
        };
        await channel.BasicConsumeAsync(_descriptor.QueueName, false, consumer, intakeToken);
        _ready.TrySetResult();
        try { await Task.Delay(Timeout.Infinite, intakeToken); }
        catch (OperationCanceledException) when (intakeToken.IsCancellationRequested) { }
        finally
        {
            try
            {
                foreach (var activeTag in consumer.ConsumerTags.ToArray())
                    await channel.BasicCancelAsync(activeTag, cancellationToken: CancellationToken.None);
                await unregistered.Task;
            }
            finally
            {
                Task idle;
                lock (_gate) idle = _idle.Task;
                await idle;
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            if (_channel is not null) await _channel.DisposeAsync();
        }
        finally
        {
            try { if (_connection is not null) await _connection.DisposeAsync(); }
            finally { _dispatchGate.Dispose(); }
        }
    }

    private sealed class RabbitDelivery : IDelivery
    {
        private readonly RabbitMqMessagePump _owner;
        private readonly BasicDeliverEventArgs _args;
        private readonly int _generation;
        public ReadOnlyMemory<byte> Payload { get; }
        public MessageHeaders Headers { get; } = new();
        public Exception? DecodeError { get; }
        public RabbitDelivery(RabbitMqMessagePump owner, BasicDeliverEventArgs args)
        {
            _owner = owner; _args = args; _generation = Volatile.Read(ref owner._generation); Payload = args.Body.ToArray();
            try { Headers = RabbitMqHeaders.ParseAmqpHeaders(args.BasicProperties.Headers); }
            catch (Exception error) { DecodeError = error; }
        }
        public async Task<bool> SettleAsync(DispatchResult result, CancellationToken cancellationToken)
        {
            var channel = _owner._channel!;
            if (!channel.IsOpen || _generation != Volatile.Read(ref _owner._generation)) return false;
            switch (result.Kind)
            {
                case DispatchResultKind.Success:
                    await channel.BasicAckAsync(_args.DeliveryTag, false, cancellationToken); break;
                case DispatchResultKind.Release:
                    await channel.BasicNackAsync(_args.DeliveryTag, false, true, cancellationToken); break;
                case DispatchResultKind.DeadLetter:
                    await _owner.DeadLetterAsync(channel, _owner._descriptor, _args, result, cancellationToken); break;
                default: throw new ArgumentOutOfRangeException(nameof(result));
            }
            return _generation == Volatile.Read(ref _owner._generation);
        }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private async Task<IConnection> ConnectWithRetryAsync(CancellationToken cancellationToken)
    {
        var attempt = 0;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                return await connectionProvider.CreateConsumeConnectionAsync(cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex) when (IsConnectionRelated(ex) && ShouldRetry(ex, attempt + 1))
            {
                var delay = options.ConnectionRetry.DelayProvider(++attempt);
                logger.LogWarning(
                    ex,
                    "InterBus RabbitMQ: consume connection failed; retrying in {Delay}",
                    delay);
                await Task.Delay(delay, cancellationToken);
            }
        }
    }

    private bool ShouldRetry(Exception ex, int attempt)
    {
        var retry = options.ConnectionRetry;
        if (attempt > retry.RetryCount)
            return false;
        return retry.ShouldRetry?.Invoke(ex) ?? true;
    }

    private static bool IsConnectionRelated(Exception ex) =>
        ex is BrokerUnreachableException
            or ConnectFailureException
            or System.Net.Sockets.SocketException
            or TimeoutException
        || (ex.InnerException is not null && IsConnectionRelated(ex.InnerException));

    private async Task DeadLetterAsync(
        IChannel consumeChannel,
        ConsumerDescriptor descriptor,
        BasicDeliverEventArgs ea,
        DispatchResult result, CancellationToken cancellationToken)
    {
        var deadLetterExchangeName = descriptor.QueueName + options.DeadLetterExchangeSuffix;
        var publishConnection = await connectionProvider.GetPublishConnectionAsync(cancellationToken);
        await using var publishChannel =
            await publishConnection.CreateChannelAsync(cancellationToken: cancellationToken);
        var properties = new BasicProperties
        {
            ContentType = ea.BasicProperties.ContentType,
            ContentEncoding = ea.BasicProperties.ContentEncoding,
            DeliveryMode = ea.BasicProperties.DeliveryMode,
            Priority = ea.BasicProperties.Priority,
            CorrelationId = ea.BasicProperties.CorrelationId,
            MessageId = ea.BasicProperties.MessageId,
            Timestamp = ea.BasicProperties.Timestamp,
            Type = ea.BasicProperties.Type,
            AppId = ea.BasicProperties.AppId,
            Headers = ea.BasicProperties.Headers is null
                ? new Dictionary<string, object?>()
                : new Dictionary<string, object?>(ea.BasicProperties.Headers, StringComparer.Ordinal),
        };

        properties.Headers[InterBusHeaders.DeadLetterReason] = result.DeadLetterReason;
        properties.Headers[InterBusHeaders.DeadLetterError] = result.DeadLetterError;
        properties.Headers[InterBusHeaders.DeadLetterTime] = result.DeadLetteredAtUtc!.Value.ToString("O");

        await publishChannel.BasicPublishAsync(
            exchange: deadLetterExchangeName,
            routingKey: descriptor.QueueName,
            mandatory: false,
            basicProperties: properties,
            body: ea.Body,
            cancellationToken: cancellationToken);

        await consumeChannel.BasicAckAsync(ea.DeliveryTag, multiple: false, cancellationToken: cancellationToken);

    }
}

internal sealed class RabbitMqSourceFactory(IServiceProvider provider) : IConsumerSourceFactory
{
    public IConsumerSource Create(ConsumerDescriptor descriptor) =>
        ActivatorUtilities.CreateInstance<RabbitMqMessagePump>(provider, descriptor);
}
