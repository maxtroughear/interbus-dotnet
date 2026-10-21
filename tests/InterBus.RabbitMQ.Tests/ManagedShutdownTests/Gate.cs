using System.Collections.Concurrent;

namespace InterBus.RabbitMQ.Tests;

public partial class ManagedShutdownTests
{
    public sealed class Gate
    {
        public int Active, Maximum;
        public ConcurrentDictionary<Guid, int> Received { get; } = new();
        public ConcurrentQueue<CancellationToken> Tokens { get; } = new();
        public TaskCompletionSource AllReceived { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
