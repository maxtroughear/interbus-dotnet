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
    private readonly TaskCompletionSource<Exception> _fault = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private Task? _start;
    private Task? _stop;
    private bool _stopping;
    private bool _stopExited;
    private bool _initializationFinished;
    private int _disposed;
    private PumpState _state;

    /// <summary>Current lifecycle state. Faults remain available for diagnostics.</summary>
    public PumpState State { get { lock (_gate) return _state; } }
    /// <summary>Consumer sharing this lifetime's sources and processing budget.</summary>
    public ConsumerDescriptor Consumer { get; } = consumer;
    /// <summary>Unhandled initialization, runtime or cleanup failure.</summary>
    public Exception? Failure { get; private set; }
    /// <summary>Completes after resources are no longer used and have been disposed. Faults are propagated to the host.</summary>
    public Task Completion => _completion.Task;
    /// <summary>Reports a fault immediately, even while accepted deliveries are still draining.</summary>
    public Task<Exception> Fault => _fault.Task;

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
        var runtimeStarted = false;
        using var initialization = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _intake.Token);
        try
        {
            await source.InitializeAsync(initialization.Token);
            initialization.Token.ThrowIfCancellationRequested();
            runtimeStarted = true;
            _ = SuperviseAsync();
            var ready = source.Ready;
            if (await Task.WhenAny(ready, Completion).WaitAsync(initialization.Token) == Completion)
            {
                await Completion;
                throw new OperationCanceledException(_intake.Token);
            }
            await ready;
            lock (_gate)
            {
                initialization.Token.ThrowIfCancellationRequested();
                if (_state == PumpState.Faulted) throw Failure!;
                if (_stopping) throw new OperationCanceledException(_intake.Token);
                _state = PumpState.Running;
            }
        }
        catch (Exception error)
        {
            if (runtimeStarted)
            {
                if (error is not OperationCanceledException || !initialization.IsCancellationRequested)
                    ReportFailure(error);
                await _intake.CancelAsync();
                try { await Completion.WaitAsync(cancellationToken); }
                catch { /* Preserve startup failure; completion retains cleanup failures */ }
            }
            else await FinishAsync(error);
            if (error is not OperationCanceledException || !_stopping) throw;
        }
        finally
        {
            lock (_gate)
            {
                _initializationFinished = true;
                if (_stopExited && Completion.IsCompleted) DisposeResources();
            }
        }
    }

    private async Task SuperviseAsync()
    {
        Exception? failure = null;
        try
        {
            var runtime = new ConsumerRuntime(Consumer, processor, ReportFailure);
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
        failure ??= Failure;
        try { await source.DisposeAsync(); }
        catch (Exception error) { failure = failure is null ? error : new AggregateException(failure, error); }
        lock (_gate)
        {
            Failure = failure;
            if (failure is not null) _fault.TrySetResult(failure);
            _state = failure is null ? PumpState.Stopped : PumpState.Faulted;
        }
        if (failure is null) _completion.TrySetResult();
        else _completion.TrySetException(failure);
        lock (_gate) { if (_stopExited && _initializationFinished) DisposeResources(); }
    }

    private void ReportFailure(Exception error)
    {
        lock (_gate)
        {
            Failure ??= error;
            _state = PumpState.Faulted;
            _fault.TrySetResult(error);
        }
        _intake.Cancel();
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
        var deadline = cancellationToken.Register(() => _processing.Cancel());
        try
        {
            await _intake.CancelAsync();
            if (_start is null) { _initializationFinished = true; await FinishAsync(null); }
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
        finally
        {
            deadline.Dispose();
            lock (_gate)
            {
                _stopExited = true;
                if (Completion.IsCompleted && _initializationFinished) DisposeResources();
            }
        }
    }

    private void DisposeResources()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        _intake.Dispose();
        _processing.Dispose();
    }

    /// <summary>Stops and disposes cancellation resources only after all transport work has finished.</summary>
    public async ValueTask DisposeAsync()
    {
        try { await StopAsync(); }
        finally
        {
            if (Completion.IsCompleted && _initializationFinished)
            {
                DisposeResources();
            }
        }
    }
}
