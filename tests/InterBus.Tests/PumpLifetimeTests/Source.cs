using InterBus.MessagePumps;

namespace InterBus.Tests;

public partial class PumpLifetimeTests
{
    private sealed class Source : IManagedSource
    {
        public bool BlockReady, BlockInitialize, FailRun, CompleteRun, IgnoreIntake, Ran, Disposed;
        public Exception? InitializationFailure, DisposalFailure;
        public CancellationToken ProcessingToken;
        private readonly TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public Task Ready => BlockReady ? _ready.Task : Task.CompletedTask;
        public TaskCompletionSource<Exception> Health { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public Task<Exception> Failure => Health.Task;
        public TaskCompletionSource Initializing { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Finish { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task InitializeAsync(CancellationToken cancellationToken)
        {
            Initializing.SetResult();
            if (InitializationFailure is not null)
                throw InitializationFailure;
            if (BlockInitialize)
                await Task.Delay(Timeout.Infinite, cancellationToken);
        }

        public async Task RunAsync(Func<IDelivery, CancellationToken, Task> process, CancellationToken intakeToken, CancellationToken processingToken)
        {
            Ran = true;
            ProcessingToken = processingToken;
            if (FailRun)
                throw new IOException();
            if (CompleteRun)
                return;
            if (IgnoreIntake)
                await Finish.Task;
            else
                await Task.Delay(Timeout.Infinite, intakeToken);
        }

        public ValueTask DisposeAsync()
        {
            Disposed = true;
            return DisposalFailure is null ? ValueTask.CompletedTask : ValueTask.FromException(DisposalFailure);
        }
    }
}
