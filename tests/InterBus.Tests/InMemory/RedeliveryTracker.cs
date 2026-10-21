using System.Collections.Concurrent;
using InterBus.Retry;

namespace InterBus.Tests.InMemory;

public sealed class RedeliveryTracker
{
    private readonly ConcurrentDictionary<Guid, int> _attemptCounts = [];
    private readonly ConcurrentBag<Guid> _successes = [];
    private readonly ConcurrentBag<string?> _seenRetryHeaders = [];
    private readonly ConcurrentBag<string?> _seenDelayRetryHeaders = [];
    public IReadOnlyCollection<Guid> Successes => _successes;
    public IReadOnlyCollection<string?> SeenRetryHeaders => _seenRetryHeaders;
    public IReadOnlyCollection<string?> SeenDelayRetryHeaders => _seenDelayRetryHeaders;

    public int GetAttemptCount(Guid messageId) => _attemptCounts.GetOrAdd(messageId, 0);
    public int RecordAttempt(Guid messageId)
    {
        var updated = _attemptCounts.AddOrUpdate(messageId, 1, (_, count) => count + 1);
        return updated;
    }

    public void RecordSuccess(Guid messageId) => _successes.Add(messageId);
    public void RecordRetryHeader(IReadOnlyDictionary<string, string> headers)
    {
        headers.TryGetValue(RedeliveryImmediateConsumeMiddleware.RetryCountHeader, out var value);
        _seenRetryHeaders.Add(value);
    }

    public void RecordDelayRetryHeader(IReadOnlyDictionary<string, string> headers)
    {
        headers.TryGetValue(RedeliveryDelayConsumeMiddleware.RetryCountHeader, out var value);
        _seenDelayRetryHeaders.Add(value);
    }
}
