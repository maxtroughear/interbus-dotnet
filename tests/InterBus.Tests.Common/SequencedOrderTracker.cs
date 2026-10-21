using System.Collections.Concurrent;

namespace InterBus.Tests.Common;

public sealed class SequencedOrderTracker
{
    private readonly ConcurrentQueue<SequencedOrderObservation> _observations = [];
    public IReadOnlyCollection<SequencedOrderObservation> Observations => _observations.ToArray();
    public int Count => _observations.Count;

    public void Track(SequencedOrder message) => _observations.Enqueue(new SequencedOrderObservation(message.MessageGroupId, message.Sequence, DateTimeOffset.UtcNow));
}
