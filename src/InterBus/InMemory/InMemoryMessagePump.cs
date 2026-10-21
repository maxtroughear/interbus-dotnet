using InterBus.Consuming;
using InterBus.MessagePumps;

namespace InterBus.InMemory;

internal sealed partial class InMemoryMessagePump(ConsumerDescriptor descriptor, InMemoryBroker broker) : IManagedSource
{
    private InMemoryQueue _queue = null!;
    public Task InitializeAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (descriptor.PumpCount != 1) throw new InvalidOperationException("In-memory consumers support PumpCount = 1 only");
        _queue = broker.EnsureQueue(descriptor);
        return Task.CompletedTask;
    }

    public Task RunAsync(Func<IDelivery, CancellationToken, Task> process, CancellationToken intakeToken,
        CancellationToken processingToken) => ConsumerRuntime.RunWorkersAsync(descriptor.ConcurrencyCount,
        async token =>
        {
            await foreach (var envelope in _queue.ReadAllAsync(token))
            {
                for (var attempt = 0; attempt <= descriptor.MaxDeliveryCount; attempt++)
                {
                    var delivery = new MemoryDelivery(envelope);
                    await process(delivery, processingToken);
                    if (!delivery.Released || processingToken.IsCancellationRequested) break;
                    await Task.Delay(_queue.DeliveryTimeout, processingToken);
                }
            }
        }, intakeToken);

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
