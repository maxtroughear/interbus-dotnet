namespace InterBus.MessagePumps;

internal sealed class CallbackIntake(ICallbackSource source) : ICallbackSource
{
    private readonly SemaphoreSlim _control = new(1);
    private readonly TaskCompletionSource<Exception> _failure = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _stopping;
    private bool _started;
    private bool _paused;
    public Task Ready => source.Ready;
    public Task<Exception> Failure
    {
        get
        {
            var health = source.Failure;
            // A pending race on the shared never-task would root this adapter after shutdown
            return ReferenceEquals(health, SourceHealth.Never) ? _failure.Task : ObserveFailureAsync(health);
        }
    }
    private async Task<Exception> ObserveFailureAsync(Task<Exception> health) => await await Task.WhenAny(health, _failure.Task);
    public Task InitializeAsync(CancellationToken token) => source.InitializeAsync(token);
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public Task StartAsync(DeliveryHandler process, CancellationToken processingToken, CancellationToken operationToken) =>
        ControlAsync(async () =>
        {
            if (_started) throw new InvalidOperationException("Callback intake already started");
            _started = true;
            await source.StartAsync(process, processingToken, operationToken);
        }, operationToken, starting: true);

    public Task PauseAsync(CancellationToken operationToken) =>
        ControlAsync(async () =>
        {
            if (!_started) throw new InvalidOperationException("Callback intake has not started");
            if (_paused) return;
            await source.PauseAsync(operationToken);
            _paused = true;
        }, operationToken);
    public Task ResumeAsync(CancellationToken operationToken) =>
        ControlAsync(async () =>
        {
            if (!_started) throw new InvalidOperationException("Callback intake has not started");
            if (!_paused) return;
            await source.ResumeAsync(operationToken);
            _paused = false;
        }, operationToken);

    internal void BeginStop() => Interlocked.Exchange(ref _stopping, 1);

    public async Task StopAsync()
    {
        BeginStop();
        await _control.WaitAsync();
        try { await source.PauseAsync(CancellationToken.None); }
        finally { _control.Release(); }
    }

    private async Task ControlAsync(Func<Task> action, CancellationToken token, bool starting = false)
    {
        await _control.WaitAsync(token);
        try
        {
            if (Volatile.Read(ref _stopping) != 0)
            {
                token.ThrowIfCancellationRequested();
                throw new InvalidOperationException("Callback intake is stopping");
            }
            try { await action(); }
            // The runtime follows canceled startup with terminal pause and drain
            catch (OperationCanceledException) when (starting && token.IsCancellationRequested) { throw; }
            catch (Exception error) { _failure.TrySetResult(error); throw; }
        }
        finally { _control.Release(); }
    }
}
