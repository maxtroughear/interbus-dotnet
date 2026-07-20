using InterBus.Consuming;
using InterBus.MessagePumps;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.AmazonSqs;

public class SqsQueueMessagePumpFactory(IServiceProvider serviceProvider) : IMessagePumpFactory
{
    public IMessagePump Create(ConsumerDescriptor descriptor)
    {
        var envelope = serviceProvider.GetKeyedService<ISnsSqsMessageEnvelope>(descriptor.ConsumerType) ??
                       serviceProvider.GetRequiredService<ISnsSqsMessageEnvelope>();
        return ActivatorUtilities.CreateInstance<SqsQueueMessagePump>(serviceProvider, descriptor, envelope);
    }
}
