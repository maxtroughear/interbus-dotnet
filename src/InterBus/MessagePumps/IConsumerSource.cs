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
