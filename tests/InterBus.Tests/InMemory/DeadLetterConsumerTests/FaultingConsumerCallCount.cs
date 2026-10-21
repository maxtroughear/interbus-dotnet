namespace InterBus.Tests.InMemory;

public partial class DeadLetterConsumerTests
{
    // ---- Support types -------------------------------------------------------
    public sealed class FaultingConsumerCallCount
    {
        private int _count;
        public int Count => _count;

        public void Increment() => Interlocked.Increment(ref _count);
    }
}
