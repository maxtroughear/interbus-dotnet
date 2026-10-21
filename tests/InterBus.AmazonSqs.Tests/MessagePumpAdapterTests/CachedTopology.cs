namespace InterBus.AmazonSqs.Tests;

public sealed partial class MessagePumpAdapterTests
{
    private sealed class CachedTopology : ITopologyBuilder
    {
        public Task BuildConsumerTopologyAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
