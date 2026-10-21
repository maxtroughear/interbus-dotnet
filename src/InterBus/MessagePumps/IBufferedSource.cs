namespace InterBus.MessagePumps;

/// <summary>A polling source. One instance owns all receive pumps for a consumer.</summary>
public interface IBufferedSource : IConsumerSource
{
    /// <summary>Additional owned deliveries beyond processing slots. Zero disables extra prefetch.</summary>
    int PrefetchCount { get; }

    /// <summary>Receives until intake cancellation. Reserve before receiving; ownership transfers only on successful handoff.</summary>
    Task ReceiveAsync(DeliverySink sink, CancellationToken intakeToken);
}
