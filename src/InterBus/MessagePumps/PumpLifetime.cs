using InterBus.Consuming;

namespace InterBus.MessagePumps;

public sealed class PumpLifetime(ConsumerDescriptor consumer, IMessagePump pump) : IDisposable
{
    public PumpState State { get; private set; } = PumpState.NotStarted;
    public ConsumerDescriptor Consumer { get; } = consumer;

    private Task? _pumpLoopTask;
    private CancellationTokenSource? _cancellationTokenSource;

    public async Task StartAsync(CancellationToken stoppingToken)
    {
        lock (this)
        {
            if (State is not PumpState.NotStarted)
                return;

            State = PumpState.Starting;
        }

        _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);

        await pump.InitializeAsync(_cancellationTokenSource.Token);

        _pumpLoopTask = pump.PumpLoopAsync(_cancellationTokenSource.Token);

        lock (this)
        {
            if (State is not PumpState.Starting) // probably stopped during initialisation
                return;

            State = PumpState.Running;
        }
    }

    public async Task StopAsync()
    {
        lock (this)
        {
            if (State is not PumpState.Running and not PumpState.Starting)
                return;

            State = PumpState.Stopping;
        }

        if (_cancellationTokenSource is not null)
            await _cancellationTokenSource.CancelAsync();
        await (_pumpLoopTask ?? Task.CompletedTask);

        lock (this)
        {
            State = PumpState.Stopped;
        }
    }

    public void Dispose()
    {
        _cancellationTokenSource?.Dispose();
    }
}
