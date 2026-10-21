using InterBus.Consuming;
using InterBus.MessagePumps;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.RabbitMQ;

internal sealed class RabbitMqSourceFactory(IServiceProvider provider) : IConsumerSourceFactory
{
    public IConsumerSource Create(ConsumerDescriptor descriptor) => ActivatorUtilities.CreateInstance<RabbitMqMessagePump>(provider, descriptor);
}
