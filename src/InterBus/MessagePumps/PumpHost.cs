using Microsoft.Extensions.Hosting;

namespace InterBus.MessagePumps;

internal sealed class PumpHost(InterBusOptions options, PumpController controller) : BackgroundService
{
    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        try
        {
            foreach (var consumer in options.Consumers)
                await controller.CreatePumpAsync(consumer, cancellationToken);
            await base.StartAsync(cancellationToken);
        }
        catch
        {
            try { await controller.StopAsync(cancellationToken); }
            catch { /* Preserve startup failure; controller retains cleanup failures */ }
            throw;
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var pumps = controller.Pumps;
        if (pumps.Count == 0) return;
        // WhenAny surfaces one failed consumer without waiting for unrelated consumers to stop
        var remaining = pumps.Select(ObserveAsync).ToList();
        while (remaining.Count > 0)
        {
            var completed = await Task.WhenAny(remaining);
            await completed;
            remaining.Remove(completed);
        }
    }

    private static async Task ObserveAsync(PumpLifetime pump)
    {
        if (await Task.WhenAny(pump.Completion, pump.Fault) == pump.Fault)
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(await pump.Fault).Throw();
        await pump.Completion;
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        try { await controller.StopAsync(cancellationToken); }
        finally { await base.StopAsync(cancellationToken); }
    }
}
