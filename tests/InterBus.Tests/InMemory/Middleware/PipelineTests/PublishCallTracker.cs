using System.Collections.Concurrent;

namespace InterBus.Tests.InMemory.Middleware;

public partial class PipelineTests
{
    /// <summary>
    /// Tracks which destinations were seen by publish middleware so tests can
    /// assert the middleware ran in the expected order.
    /// </summary>
    public sealed class PublishCallTracker
    {
        private readonly ConcurrentBag<string> _calls = [];
        public IReadOnlyCollection<string> Calls => _calls;

        public void Record(string name) => _calls.Add(name);
    }
}
