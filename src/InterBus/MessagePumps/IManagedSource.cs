namespace InterBus.MessagePumps;

/// <summary>A callback or transaction owning source, with transport configured total consumer concurrency.</summary>
public interface IManagedSource : IConsumerSource
{
    /// <summary>Runs intake and awaits processing inside every callback or transaction lifetime.</summary>
    /// <remarks>On intake cancellation stop new work and await callbacks, retaining resources until they return.
    /// The processing token is canceled only when the shutdown deadline expires.</remarks>
    Task RunAsync(Func<IDelivery, CancellationToken, Task> process, CancellationToken intakeToken, CancellationToken processingToken);
}
