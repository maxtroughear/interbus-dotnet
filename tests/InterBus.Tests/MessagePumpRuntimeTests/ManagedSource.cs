using InterBus.MessagePumps;
using Xunit;

namespace InterBus.Tests;

public partial class MessagePumpRuntimeTests
{
    private sealed class ManagedSource : IManagedSource
    {
        public Delivery Delivery { get; } = new();

        public Task InitializeAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        public async Task RunAsync(Func<IDelivery, CancellationToken, Task> process, CancellationToken intakeToken, CancellationToken processingToken)
        {
            await process(Delivery, processingToken);
            Assert.True(Delivery.Disposed);
        }
    }
}
