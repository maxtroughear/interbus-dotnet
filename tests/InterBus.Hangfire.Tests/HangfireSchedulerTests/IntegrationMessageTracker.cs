using System.Collections.Concurrent;

namespace InterBus.Hangfire.Tests;

public sealed partial class HangfireSchedulerTests
{
    internal sealed class IntegrationMessageTracker
    {
        private readonly ConcurrentBag<IntegrationMessage> _messages = [];
        public IReadOnlyCollection<IntegrationMessage> Messages => _messages;

        public void Track(IntegrationMessage msg) => _messages.Add(msg);
    }
}
