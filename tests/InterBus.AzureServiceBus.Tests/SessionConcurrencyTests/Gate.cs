using System.Collections.Concurrent;

namespace InterBus.AzureServiceBus.Tests;

public partial class SessionConcurrencyTests
{
    public sealed class Gate
    {
        public int Active, Maximum, Finished;
        public ConcurrentQueue<(string Group, int Sequence)> Started { get; } = new();
        public ConcurrentDictionary<string, int> PerSessionActive { get; } = new();
        public ConcurrentDictionary<string, int> PerSessionMaximum { get; } = new();
        public TaskCompletionSource BothEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Completed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
