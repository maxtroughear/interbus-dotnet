namespace InterBus.MessagePumps;

/// <summary>Processes an accepted delivery through settlement and disposal.</summary>
/// <param name="delivery">Delivery whose ownership transfers to the core on invocation.</param>
/// <param name="processingToken">Canceled only when the processing deadline expires.</param>
public delegate Task DeliveryHandler(IDelivery delivery, CancellationToken processingToken);

/// <summary>Acquires independent deliveries using capacity reserved by the core. Calls may run concurrently.</summary>
public interface IBufferedPollingSource : IConsumerSource
{
    /// <summary>Additional ownership slots beyond consumer concurrency.</summary>
    int PrefetchCount { get; }
    /// <summary>Maximum deliveries requested by one iteration, greater than zero.</summary>
    int MaxBatchSize { get; }
    /// <summary>Performs one acquisition. Dispose every acquired delivery that was not transferred.</summary>
    /// <param name="batch">Iteration reservation; transfer never waits for processing.</param>
    /// <param name="intakeToken">Cancels acquisition, independently of accepted processing.</param>
    Task<PollResult> ReceiveOnceAsync(ReceiveBatch batch, CancellationToken intakeToken);
}

/// <summary>Retains a resource scope through processing. Concurrent calls must use independent resources.</summary>
public interface IScopedPollingSource : IConsumerSource
{
    /// <summary>Acquires at most one delivery and awaits processing and surrounding resource cleanup.</summary>
    /// <param name="process">Invoke once to transfer delivery ownership. Failures after invocation are never acquisition retries.</param>
    /// <param name="intakeToken">Cancels acquisition and idle waits.</param>
    /// <param name="processingToken">Cancels accepted processing only at its deadline.</param>
    Task<PollResult> ProcessOnceAsync(DeliveryHandler process, CancellationToken intakeToken, CancellationToken processingToken);
}

/// <summary>Controls SDK intake while retaining resources for accepted callbacks.</summary>
public interface ICallbackSource : IConsumerSource
{
    /// <summary>Installs callbacks once and starts intake. Returns after startup succeeds.</summary>
    /// <param name="process">Transfers ownership on callback invocation and awaits settlement and cleanup.</param>
    /// <param name="processingToken">Canceled only at the accepted processing deadline.</param>
    /// <param name="operationToken">Bounds startup; failures must leave intake stopped or publish a fatal failure.</param>
    Task StartAsync(DeliveryHandler process, CancellationToken processingToken, CancellationToken operationToken);
    /// <summary>Stops new intake and drains scheduled callbacks without canceling accepted processing.</summary>
    /// <param name="operationToken">Bounds the control operation. On failure, report fatal health if intake state is uncertain.</param>
    Task PauseAsync(CancellationToken operationToken);
    /// <summary>Restarts intake without registering duplicate callbacks.</summary>
    /// <param name="operationToken">Bounds restart. On failure, report fatal health if intake state is uncertain.</param>
    Task ResumeAsync(CancellationToken operationToken);
}

/// <summary>Acquisition outcome, independent of acknowledgement.</summary>
/// <param name="FoundWork">Whether an acquisition found a delivery, including released or dead-lettered work.</param>
/// <param name="IdleWait">Deferred empty-poll wait, invoked only after unused capacity and acquisition resources are released.</param>
public readonly record struct PollResult(bool FoundWork, Func<CancellationToken, ValueTask>? IdleWait = null);

/// <summary>Optional transport classification of acquisition failures. Processing and cleanup failures after acceptance bypass this policy.</summary>
public interface IPollingRetryPolicy
{
    /// <summary>Returns a nonnegative retry delay, or null for terminal failure. Attempt numbers start at one and reset after successful acquisition.</summary>
    /// <param name="error">Acquisition exception, before scoped delivery acceptance.</param>
    /// <param name="attempt">Consecutive failed acquisition attempt, starting at one.</param>
    /// <returns>A nonnegative delay for retry, or null to stop the runtime.</returns>
    TimeSpan? GetRetryDelay(Exception error, int attempt);
}
