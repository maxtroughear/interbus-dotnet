namespace InterBus.Tests.InMemory;

public partial class InMemoryOutboxTests
{
    internal class DelayedConsumerController
    {
        internal SemaphoreSlim Semaphore { get; } = new(1);
        internal bool Published { get; set; }
        internal bool ShouldFail { get; set; }
    }
}
