using InterBus.Consuming;
using InterBus.MessagePumps;

namespace InterBus.Tests;

public partial class PumpLifetimeTests
{
    private sealed class Factory(Source source) : IConsumerSourceFactory
    {
        public IConsumerSource Create(ConsumerDescriptor descriptor) => source;
    }
}
