namespace InterBus.AmazonSqs.Tests;

public partial class DeadLetteringTests
{
    public sealed class ReceiveTracker
    {
        private int _count;
        public int Count => _count;

        public void Increment() => Interlocked.Increment(ref _count);
    }
}
