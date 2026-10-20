using InterBus.Consuming;

namespace InterBus.MessagePumps;

/// <summary>Supervises initialization, intake, graceful drain and disposal for one consumer.</summary>
public sealed class PumpLifetime(ConsumerDescriptor consumer, IConsumerSource source, DeliveryProcessor processor)
    : IAsyncDisposable
{
    private readonly object _gate = new();
    private readonly CancellationTokenSource _intake = new();
    private readonly CancellationTokenSource _processing = new();
    private readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private Task? _start;
    private Task? _stop;
    private bool _stopping;
    private PumpState _state;

    /// <summary>Current lifecycle state. Faults remain available for diagnostics.</summary>
    public PumpState State { get { lock (_gate) return _state; } }
    /// <summary>Consumer sharing this lifetime's sources and processing budget.</summary>
    public ConsumerDescriptor Consumer { get; } = consumer;
    /// <summary>Unhandled initialization, runtime or cleanup failure.</summary>
    public Exception? Failure { get; private set; }
    /// <summary>Completes after resources are no longer used and have been disposed. Faults are propagated to the host.</summary>
    public Task Completion => _completion.Task;

    /// <summary>Initializes once, honoring startup cancellation and any concurrent stop request.</summary>
    public Task StartAsync(CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            if (_stopping) return Task.CompletedTask;
            return _start ??= StartCoreAsync(cancellationToken);
        }
    }

    private async Task StartCoreAsync(CancellationToken cancellationToken)
    {
        _state = PumpState.Starting;
        using var initialization = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _intake.Token);
        try
        {
            await source.InitializeAsync(initialization.Token);
            initialization.Token.ThrowIfCancellationRequested();
            lock (_gate)
            {
                if (_stopping) throw new OperationCanceledException(_intake.Token);
                _state = PumpState.Running;
            }
            _ = SuperviseAsync();
        }
        catch (Exception error)
        {
            await FinishAsync(error);
            if (error is not OperationCanceledException || !_intake.IsCancellationRequested) throw;
        }
    }

    private async Task SuperviseAsync()
    {
        Exception? failure = null;
        try
        {
            var runtime = new ConsumerRuntime(Consumer, processor);
            await (source switch
            {
                IBufferedSource buffered => runtime.RunAsync(buffered, _intake.Token, _processing.Token),
                IManagedSource managed => runtime.RunAsync(managed, _intake.Token, _processing.Token),
                _ => throw new InvalidOperationException("Source must implement buffered or managed execution")
            });
            if (!_intake.IsCancellationRequested)
                throw new InvalidOperationException("Consumer source completed unexpectedly");
        }
        catch (OperationCanceledException) when (_intake.IsCancellationRequested || _processing.IsCancellationRequested) { }
        catch (Exception error) { failure = error; }
        await FinishAsync(failure);
    }

    private async Task FinishAsync(Exception? failure)
    {
        if (failure is OperationCanceledException && _intake.IsCancellationRequested) failure = null;
        try { await source.DisposeAsync(); }
        catch (Exception error) { failure = failure is null ? error : new AggregateException(failure, error); }
        lock (_gate)
        {
            Failure = failure;
            _state = failure is null ? PumpState.Stopped : PumpState.Faulted;
        }
        if (failure is null) _completion.TrySetResult();
        else _completion.TrySetException(failure);
    }

    /// <summary>Stops intake once and drains accepted work. Deadline cancellation cancels processing and reports incomplete shutdown.</summary>
    /// <remarks>Concurrent calls await the same shutdown. Resources remain alive if a handler ignores cancellation.</remarks>
    public Task StopAsync(CancellationToken cancellationToken = default)
    {
        lock (_gate) return _stop ??= StopCoreAsync(cancellationToken);
    }

    private async Task StopCoreAsync(CancellationToken cancellationToken)
    {
        _stopping = true;
        if (_state != PumpState.Faulted && _state != PumpState.Stopped) _state = PumpState.Stopping;
        using var deadline = cancellationToken.Register(() => _processing.Cancel());
        await _intake.CancelAsync();
        if (_start is null) await FinishAsync(null);
        else
        {
            try { await _start.WaitAsync(cancellationToken); }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            { await _processing.CancelAsync(); throw; }
            catch { /* Completion retains the initialization failure */ }
        }
        try { await Completion.WaitAsync(cancellationToken); }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            await _processing.CancelAsync();
            throw;
        }
    }

    /// <summary>Stops and disposes cancellation resources only after all transport work has finished.</summary>
    public async ValueTask DisposeAsync()
    {
        try { await StopAsync(); }
        finally
        {
            if (Completion.IsCompleted)
            {
                _intake.Dispose();
                _processing.Dispose();
            }
        }
    }
}
