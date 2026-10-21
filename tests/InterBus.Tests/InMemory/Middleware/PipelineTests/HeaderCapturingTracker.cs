using System.Collections.Concurrent;

namespace InterBus.Tests.InMemory.Middleware;

public partial class PipelineTests
{
    public sealed class HeaderCapturingTracker
    {
        private readonly ConcurrentBag<IReadOnlyDictionary<string, string>> _captured = [];
        public IReadOnlyCollection<IReadOnlyDictionary<string, string>> CapturedHeaders => _captured;

        public void Capture(IReadOnlyDictionary<string, string> headers) => _captured.Add(headers);
    }
}
