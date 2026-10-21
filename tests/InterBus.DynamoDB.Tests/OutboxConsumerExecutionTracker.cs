namespace InterBus.DynamoDB.Tests;

public sealed class OutboxConsumerExecutionTracker
{
    private int _count;
    public int Count => _count;

    public void Track() => Interlocked.Increment(ref _count);
}
