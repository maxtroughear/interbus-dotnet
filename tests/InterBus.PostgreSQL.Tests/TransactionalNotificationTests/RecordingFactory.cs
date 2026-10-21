using InterBus.Consuming;
using InterBus.MessagePumps;

namespace InterBus.PostgreSQL.Tests;

public partial class TransactionalNotificationTests
{
    private sealed class RecordingFactory(IConsumerSourceFactory inner, Gate gate) : IConsumerSourceFactory
    {
        public IConsumerSource Create(ConsumerDescriptor descriptor) => gate.Source = inner.Create(descriptor);
    }
}
