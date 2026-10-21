using InterBus.Consuming;
using InterBus.MessagePumps;
using InterBus.Dispatching;

namespace InterBus.InMemory;

internal sealed class InMemoryMessagePump(ConsumerDescriptor descriptor, InMemoryBroker broker) : IScopedPollingSource
{
    private InMemoryQueue _queue = null!;
    public Task InitializeAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (descriptor.PumpCount != 1) throw new InvalidOperationException("In-memory consumers support PumpCount = 1 only");
        _queue = broker.EnsureQueue(descriptor);
        return Task.CompletedTask;
    }

    private readonly System.Collections.Concurrent.ConcurrentQueue<(InMemoryEnvelope Envelope, int Attempt, DateTimeOffset Due)> _retries = new();

    public async Task<PollResult> ProcessOnceAsync(DeliveryHandler process, CancellationToken intakeToken,
        CancellationToken processingToken)
    {
        InMemoryEnvelope envelope;
        var attempt = 0;
        if (_retries.TryDequeue(out var retry))
        {
            envelope = retry.Envelope;
            attempt = retry.Attempt;
            var delay = retry.Due - DateTimeOffset.UtcNow;
            if (delay > TimeSpan.Zero) await Task.Delay(delay, intakeToken);
        }
        else if (!_queue.TryRead(out envelope!))
            return new PollResult(false, _queue.WaitForReadabilityAsync);
        var delivery = new MemoryDelivery(envelope);
        await process(delivery, processingToken);
        if (delivery.Released && attempt < descriptor.MaxDeliveryCount && !processingToken.IsCancellationRequested)
            _retries.Enqueue((envelope, attempt + 1, DateTimeOffset.UtcNow + _queue.DeliveryTimeout));
        return new PollResult(true);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private sealed class MemoryDelivery(InMemoryEnvelope envelope) : IDelivery
    {
        public ReadOnlyMemory<byte> Payload => envelope.Payload;
        public MessageHeaders Headers => envelope.Headers;
        public bool Released { get; private set; }
        public Task<bool> SettleAsync(DispatchResult result, CancellationToken cancellationToken)
        {
            Released = result.Kind == DispatchResultKind.Release;
            return Task.FromResult(true);
        }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
