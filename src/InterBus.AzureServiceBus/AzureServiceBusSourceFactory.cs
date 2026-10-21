using InterBus.Consuming;
using InterBus.MessagePumps;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.AzureServiceBus;

internal sealed class AzureServiceBusSourceFactory(IServiceProvider provider) : IConsumerSourceFactory
{
    public IConsumerSource Create(ConsumerDescriptor descriptor) => ActivatorUtilities.CreateInstance<AzureServiceBusMessagePump>(provider, descriptor);
}
