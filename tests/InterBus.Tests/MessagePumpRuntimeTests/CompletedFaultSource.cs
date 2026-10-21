using InterBus.MessagePumps;

namespace InterBus.Tests;

public partial class MessagePumpRuntimeTests
{
    private sealed class CompletedFaultSource(Exception error, bool canceledCompletion) : IManagedSource, IBufferedSource
    {
        public int PrefetchCount => 0;
        public Task<Exception> Failure { get; } = Task.FromResult(error);

        public Task InitializeAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        public Task ReceiveAsync(DeliverySink sink, CancellationToken intakeToken) => Complete(intakeToken);
        public Task RunAsync(Func<IDelivery, CancellationToken, Task> process, CancellationToken intakeToken, CancellationToken processingToken) => Complete(intakeToken);
        private Task Complete(CancellationToken token) => canceledCompletion ? Task.FromCanceled(token) : Task.CompletedTask;
    }
}
