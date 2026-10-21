namespace InterBus.AzureServiceBus.Tests;

/// <summary>Tracks how many times a consumer was invoked regardless of outcome.</summary>
public sealed class ReceiveTracker
{
    private int _count;
    public int Count => _count;

    public void Increment() => Interlocked.Increment(ref _count);
}
