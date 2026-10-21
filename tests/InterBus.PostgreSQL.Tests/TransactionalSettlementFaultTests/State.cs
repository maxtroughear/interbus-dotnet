using System.Collections.Concurrent;

namespace InterBus.PostgreSQL.Tests;

public partial class TransactionalSettlementFaultTests
{
    public sealed class State(string schema, string fault)
    {
        public string Schema => schema;
        public string Fault => fault;
        public ConcurrentDictionary<Guid, int> Attempts { get; } = new();
        public ConcurrentBag<long> TransactionIds { get; } = [];
        public ConcurrentBag<IConsumeContext> Contexts { get; } = [];
        public TaskCompletionSource<int> Claimed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Resume { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
