using System.Collections.Concurrent;

namespace InterBus.PostgreSQL.Tests;

public partial class TransactionContextTests
{
    public sealed class Transactions
    {
        public bool UseLedger;
        public int Attempts;
        public ConcurrentBag<(long TransactionId, int Value)> Seen { get; } = [];
    }
}
