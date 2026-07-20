namespace InterBus.MessagePumps;

public interface IMessagePump
{
    public Task InitializeAsync(CancellationToken stoppingToken);
    public Task PumpLoopAsync(CancellationToken stoppingToken);
}
