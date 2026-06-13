using System.Collections.Concurrent;
using InterBus.Sending;

namespace InterBus.EntityFrameworkCore.PostgreSQL.IntegrationTests;

public sealed class OrderPlacedTracker
{
    private readonly ConcurrentBag<OrderPlaced> _messages = [];
    public IReadOnlyCollection<OrderPlaced> Messages => _messages;
    public void Track(OrderPlaced message) => _messages.Add(message);
}

public sealed class OrderPlacedConsumer(OrderPlacedTracker tracker) : IConsumer<OrderPlaced>
{
    public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        tracker.Track(message);
        return Task.CompletedTask;
    }
}

public sealed record SequencedOrderObservation(
    string MessageGroupId,
    int Sequence,
    DateTimeOffset ConsumedAt);

public sealed class SequencedOrderTracker
{
    private readonly ConcurrentQueue<SequencedOrderObservation> _observations = [];
    public IReadOnlyCollection<SequencedOrderObservation> Observations => _observations.ToArray();
    public int Count => _observations.Count;

    public void Track(SequencedOrder message) =>
        _observations.Enqueue(new SequencedOrderObservation(
            message.MessageGroupId,
            message.Sequence,
            DateTimeOffset.UtcNow));
}

public sealed class SequencedOrderConsumer(SequencedOrderTracker tracker) : IConsumer<SequencedOrder>
{
    public Task Consume(SequencedOrder message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        tracker.Track(message);
        return Task.CompletedTask;
    }
}

public sealed class HeaderDelaySendMiddleware : ISendMiddleware
{
    public const string DelayHeaderName = "x-interbus-test-send-delay-ms";

    public async Task InvokeAsync(
        SendContext context,
        Func<SendContext, CancellationToken, Task> next,
        CancellationToken cancellationToken = default)
    {
        if (context.Headers.TryGetValue(DelayHeaderName, out var delayHeader)
            && int.TryParse(delayHeader, out var delayMs)
            && delayMs > 0)
            await Task.Delay(delayMs, cancellationToken);

        await next(context, cancellationToken);
    }
}

public sealed record SendSequenceObservation(string MessageGroupId, int Sequence);

public sealed class SendSequenceTracker
{
    private readonly ConcurrentQueue<SendSequenceObservation> _observations = [];
    public IReadOnlyCollection<SendSequenceObservation> Observations => _observations.ToArray();
    public int Count => _observations.Count;

    public void Track(string messageGroupId, int sequence) =>
        _observations.Enqueue(new SendSequenceObservation(messageGroupId, sequence));
}

public sealed class SequenceTraceSendMiddleware(SendSequenceTracker tracker) : ISendMiddleware
{
    public const string SequenceHeaderName = "x-interbus-test-sequence";

    public async Task InvokeAsync(
        SendContext context,
        Func<SendContext, CancellationToken, Task> next,
        CancellationToken cancellationToken = default)
    {
        if (context.Headers.TryGetValue(InterBusHeaders.MessageGroupId, out var messageGroupId)
            && context.Headers.TryGetValue(SequenceHeaderName, out var sequenceHeader)
            && int.TryParse(sequenceHeader, out var sequence))
            tracker.Track(messageGroupId, sequence);

        await next(context, cancellationToken);
    }
}
