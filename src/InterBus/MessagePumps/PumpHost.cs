using Microsoft.Extensions.Hosting;

namespace InterBus.MessagePumps;

internal sealed class PumpHost(InterBusOptions options, PumpController controller) : BackgroundService
{
    private Task<Exception?>[] _observers = [];
    internal Task<Exception?[]> Observations => Task.WhenAll(_observers);

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
        _observers = pumps.Select(ObserveAsync).ToArray();
        var remaining = _observers.ToList();
        while (remaining.Count > 0)
        {
            var completed = await Task.WhenAny(remaining);
            var failure = await completed;
            if (failure is not null) System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failure).Throw();
            remaining.Remove(completed);
        }
    }

    private static async Task<Exception?> ObserveAsync(PumpLifetime pump)
    {
        try
        {
            if (await Task.WhenAny(pump.Completion, pump.Fault) == pump.Fault) return await pump.Fault;
            await pump.Completion;
            return null;
        }
        catch (Exception error) { return error; }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        try { await controller.StopAsync(cancellationToken); }
        finally { await base.StopAsync(cancellationToken); }
    }
}
