using InterBus.Consuming;
using InterBus.MessagePumps;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.AmazonSqs.MessagePumps;

internal sealed class SqsQueueMessagePumpFactory(IServiceProvider serviceProvider) : IConsumerSourceFactory
{
    public IConsumerSource Create(ConsumerDescriptor descriptor)
    {
        var envelope = serviceProvider.GetKeyedService<ISnsSqsMessageEnvelope>(descriptor.ConsumerType) ??
                       serviceProvider.GetRequiredService<ISnsSqsMessageEnvelope>();
        return ActivatorUtilities.CreateInstance<SqsQueueMessagePump>(serviceProvider, descriptor, envelope);
    }
}
