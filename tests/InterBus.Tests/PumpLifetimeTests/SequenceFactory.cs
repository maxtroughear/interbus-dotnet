using InterBus.Consuming;
using InterBus.MessagePumps;

namespace InterBus.Tests;

public partial class PumpLifetimeTests
{
    private sealed class SequenceFactory(params Source[] sources) : IConsumerSourceFactory
    {
        private int _next;
        public IConsumerSource Create(ConsumerDescriptor descriptor) => sources[_next++];
    }
}
