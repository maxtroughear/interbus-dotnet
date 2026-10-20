namespace InterBus.MessagePumps;

/// <summary>A consumer's transport resources, initialized before intake and disposed after all deliveries finish.</summary>
public interface IConsumerSource : IAsyncDisposable
{
    /// <summary>Completes when intake startup succeeds. Callback adapters complete this after starting their SDK processor.</summary>
    Task Ready => Task.CompletedTask;
    /// <summary>Reports an unrecoverable background failure promptly while owned deliveries drain.</summary>
    Task<Exception> Failure => SourceHealth.Never;
    /// <summary>Initializes transport resources. Cancellation must roll back partial initialization.</summary>
    Task InitializeAsync(CancellationToken cancellationToken);
}

/// <summary>A polling source. One instance owns all receive pumps for a consumer.</summary>
public interface IBufferedSource : IConsumerSource
{
    /// <summary>Additional owned deliveries beyond processing slots. Zero disables extra prefetch.</summary>
    int PrefetchCount { get; }
    /// <summary>Receives until intake cancellation. Reserve before receiving; ownership transfers only on successful handoff.</summary>
    Task ReceiveAsync(DeliverySink sink, CancellationToken intakeToken);
}

/// <summary>A callback or transaction owning source, with transport configured total consumer concurrency.</summary>
public interface IManagedSource : IConsumerSource
{
    /// <summary>Runs intake and awaits processing inside every callback or transaction lifetime.</summary>
    /// <remarks>On intake cancellation stop new work and await callbacks, retaining resources until they return.
    /// The processing token is canceled only when the shutdown deadline expires.</remarks>
    Task RunAsync(Func<IDelivery, CancellationToken, Task> process, CancellationToken intakeToken,
        CancellationToken processingToken);
}

internal static class SourceHealth
{
    internal static readonly Task<Exception> Never = new TaskCompletionSource<Exception>().Task;
}
