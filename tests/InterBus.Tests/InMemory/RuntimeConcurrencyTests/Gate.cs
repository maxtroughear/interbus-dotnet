namespace InterBus.Tests.InMemory;

public partial class RuntimeConcurrencyTests
{
    public sealed class Gate(int expected)
    {
        public int Expected => expected;

        public int Active, Maximum, Finished;
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Done { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
