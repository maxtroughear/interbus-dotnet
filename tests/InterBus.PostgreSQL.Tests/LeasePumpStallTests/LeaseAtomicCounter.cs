namespace InterBus.PostgreSQL.Tests;

public partial class LeasePumpStallTests
{
    public sealed class LeaseAtomicCounter
    {
        private int _value;
        public int Value => _value;

        public int Increment() => Interlocked.Increment(ref _value);
    }
}
