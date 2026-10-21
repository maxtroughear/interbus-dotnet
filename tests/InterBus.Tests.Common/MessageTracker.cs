using System.Collections.Concurrent;

namespace InterBus.Tests.Common;

/// <summary>
/// Singleton-safe tracker registered in DI so all consumer scopes write to the same list.
/// </summary>
public class MessageTracker<T>
    where T : class
{
    private readonly ConcurrentBag<T> _messages = [];
    public IReadOnlyCollection<T> Messages => _messages;

    public void Track(T message) => _messages.Add(message);
}
