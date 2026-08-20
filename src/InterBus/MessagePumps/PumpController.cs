using InterBus.Consuming;
using Microsoft.Extensions.Hosting;

namespace InterBus.MessagePumps;

public class PumpController(IMessagePumpFactory messagePumpFactory, IHostApplicationLifetime appLifetime)
{
    public IReadOnlyList<PumpLifetime> Pumps
    {
        get
        {
            IReadOnlyList<PumpLifetime> pumps;
            lock (_pumps)
            {
                pumps = _pumps.AsReadOnly();
            }

            return pumps;
        }
    }

    private readonly List<PumpLifetime> _pumps = [];
    private readonly CancellationToken _stoppingToken = appLifetime.ApplicationStopping;

    public async Task<PumpLifetime> CreatePumpAsync(ConsumerDescriptor consumer)
    {
        var pump = new PumpLifetime(consumer, messagePumpFactory.Create(consumer));
        lock (_pumps)
        {
            _pumps.Add(pump);
        }

        await pump.StartAsync(_stoppingToken);
        return pump;
    }

    public static Task StopPumpAsync(PumpLifetime pump)
    {
        return pump.StopAsync();
    }

    public Task StopPumpsForConsumerAsync(ConsumerDescriptor consumer)
    {
        List<PumpLifetime> pumpsToStop;
        lock (_pumps)
        {
            pumpsToStop = _pumps.Where(p => p.Consumer == consumer).ToList();
        }

        return Task.WhenAll(pumpsToStop.Select(p => p.StopAsync()));
    }

    public void CleanupStoppedPumps()
    {
        lock (_pumps)
        {
            _pumps.RemoveAll(p => p.State == PumpState.Stopped);
        }
    }
}