namespace InterBus.PostgreSQL.Tests;

public partial class LeaseConcurrencyBoundTests
{
    public sealed class Gate
    {
        public int Active, Maximum, Finished;
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Completed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
