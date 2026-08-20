using Microsoft.Extensions.Hosting;

namespace InterBus.MessagePumps;

internal class PumpHost(InterBusOptions options, PumpController pumpController) : BackgroundService, IHostedService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        foreach (var consumer in options.Consumers)
        {
            for (var i = 0; i < consumer.PumpCount; i++)
                await pumpController.CreatePumpAsync(consumer);
        }
        
        await Task.Delay(Timeout.Infinite, stoppingToken);
        
        foreach (var pump in pumpController.Pumps)
        {
            await pump.StopAsync();
        }
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        // Will take a little bit of time to initialise pumps, blocking start of other hosted services
        foreach (var consumer in options.Consumers)
        {
            for (var i = 0; i < consumer.PumpCount; i++)
                await pumpController.CreatePumpAsync(consumer);
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        foreach (var pump in pumpController.Pumps)
        {
            await pump.StopAsync();
        }
    }
}
