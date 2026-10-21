namespace InterBus.Tests.InMemory.Middleware;

public partial class PipelineTests
{
    public sealed class ConsumeMiddlewareCallTracker
    {
        private int _count;
        public int InvocationCount => _count;

        public void Increment() => Interlocked.Increment(ref _count);
    }
}
