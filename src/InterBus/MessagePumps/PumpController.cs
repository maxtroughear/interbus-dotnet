using InterBus.Consuming;

namespace InterBus.MessagePumps;

/// <summary>Owns one supervised runtime per consumer and exposes diagnostic snapshots.</summary>
public sealed class PumpController(IConsumerSourceFactory factory, DeliveryProcessor processor)
{
    private readonly List<PumpLifetime> _pumps = [];
    /// <summary>A snapshot retaining terminal states and failures for diagnostics.</summary>
    public IReadOnlyList<PumpLifetime> Pumps { get { lock (_pumps) return _pumps.ToArray(); } }

    /// <summary>Creates and initializes a consumer runtime before returning.</summary>
    public async Task<PumpLifetime> CreatePumpAsync(ConsumerDescriptor consumer, CancellationToken cancellationToken = default)
    {
        PumpLifetime pump;
        lock (_pumps)
        {
            pump = _pumps.FirstOrDefault(existing => existing.Consumer == consumer &&
                !existing.Completion.IsCompleted)!;
            if (pump is null)
            {
                pump = new PumpLifetime(consumer, factory.Create(consumer), processor);
                _pumps.Add(pump);
            }
        }
        await pump.StartAsync(cancellationToken);
        return pump;
    }

    /// <summary>Stops every runtime, observing all failures before reporting them.</summary>
    public Task StopAsync(CancellationToken cancellationToken) =>
        StopAllAsync(Pumps, cancellationToken);

    /// <summary>Stops all runtimes for a consumer.</summary>
    public Task StopPumpsForConsumerAsync(ConsumerDescriptor consumer) =>
        StopAllAsync(Pumps.Where(pump => pump.Consumer == consumer), default);

    private static async Task StopAllAsync(IEnumerable<PumpLifetime> pumps, CancellationToken cancellationToken)
    {
        var stops = pumps.Select(pump => StopAndDisposeAsync(pump, cancellationToken)).ToArray();
        try { await Task.WhenAll(stops); }
        catch
        {
            var failures = stops.Where(task => task.IsFaulted)
                .SelectMany(task => task.Exception!.InnerExceptions).ToArray();
            if (failures.Length > 1) throw new AggregateException("Consumer shutdown failed", failures);
            throw;
        }
    }

    private static async Task StopAndDisposeAsync(PumpLifetime pump, CancellationToken cancellationToken)
    {
        try { await pump.StopAsync(cancellationToken); }
        finally { if (pump.Completion.IsCompleted) await pump.DisposeAsync(); }
    }
}
