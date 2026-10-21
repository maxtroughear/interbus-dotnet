namespace InterBus.Tests.InMemory.Middleware;

public partial class PipelineTests
{
    public sealed class FaultingConsumerCallCount
    {
        private int _count;
        public int Count => _count;

        public void Increment() => Interlocked.Increment(ref _count);
    }
}
