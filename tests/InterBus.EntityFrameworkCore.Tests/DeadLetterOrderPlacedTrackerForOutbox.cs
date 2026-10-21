using System.Collections.Concurrent;
using InterBus.DeadLettering;

namespace InterBus.EntityFrameworkCore.Tests;

/// <summary>
/// Tracker for <see cref = "DeadLetter{T}"/> messages in the outbox test project.
/// Uses a distinct type to avoid singleton conflicts with other test projects.
/// </summary>
public sealed class DeadLetterOrderPlacedTrackerForOutbox
{
    private readonly ConcurrentBag<DeadLetter<OrderPlaced>> _messages = [];
    public IReadOnlyCollection<DeadLetter<OrderPlaced>> Messages => _messages;

    public void Track(DeadLetter<OrderPlaced> msg) => _messages.Add(msg);
}
