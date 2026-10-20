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
        var pump = new PumpLifetime(consumer, factory.Create(consumer), processor);
        lock (_pumps) _pumps.Add(pump);
        await pump.StartAsync(cancellationToken);
        return pump;
    }

    /// <summary>Stops every runtime, observing all failures before reporting them.</summary>
    public Task StopAsync(CancellationToken cancellationToken) =>
        Task.WhenAll(Pumps.Select(pump => StopAndDisposeAsync(pump, cancellationToken)));

    /// <summary>Stops all runtimes for a consumer.</summary>
    public Task StopPumpsForConsumerAsync(ConsumerDescriptor consumer) =>
        Task.WhenAll(Pumps.Where(pump => pump.Consumer == consumer).Select(pump => StopAndDisposeAsync(pump, default)));

    private static async Task StopAndDisposeAsync(PumpLifetime pump, CancellationToken cancellationToken)
    {
        try { await pump.StopAsync(cancellationToken); }
        finally { if (pump.Completion.IsCompleted) await pump.DisposeAsync(); }
    }
}
