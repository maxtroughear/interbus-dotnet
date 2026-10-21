using InterBus.MessagePumps;

namespace InterBus.PostgreSQL.Tests;

public partial class TransactionalNotificationTests
{
    public sealed class Gate
    {
        public IConsumerSource? Source;
        public int Active, Finished;
        public TaskCompletionSource AllEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Completed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
