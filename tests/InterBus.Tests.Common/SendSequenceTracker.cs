using System.Collections.Concurrent;

namespace InterBus.Tests.Common;

public sealed class SendSequenceTracker
{
    private readonly ConcurrentQueue<SendSequenceObservation> _observations = [];
    public IReadOnlyCollection<SendSequenceObservation> Observations => _observations.ToArray();
    public int Count => _observations.Count;

    public void Track(string messageGroupId, int sequence) => _observations.Enqueue(new SendSequenceObservation(messageGroupId, sequence));
}
