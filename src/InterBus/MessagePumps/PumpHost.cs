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
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        foreach (var consumer in options.Consumers)
        {
            for (var i = 0; i < consumer.PumpCount; i++)
                await pumpController.CreatePumpAsync(consumer);
        }
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }
}
