using System.Collections.Concurrent;

namespace InterBus.Tests.InMemory;

public partial class HeadersTests
{
    /// <summary>Captures headers seen by the consumer during Consume.</summary>
    public sealed class HeaderCapture
    {
        private readonly ConcurrentBag<IReadOnlyDictionary<string, string>> _captured = [];
        public IReadOnlyCollection<IReadOnlyDictionary<string, string>> Captured => _captured;

        public void Add(IReadOnlyDictionary<string, string> headers) => _captured.Add(headers);
    }
}
