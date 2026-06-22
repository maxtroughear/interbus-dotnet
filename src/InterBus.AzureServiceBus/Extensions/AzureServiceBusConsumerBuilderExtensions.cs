using Azure.Messaging.ServiceBus.Administration;
using InterBus.Consuming;

namespace InterBus.AzureServiceBus.Extensions;

public static class AzureServiceBusConsumerBuilderExtensions
{
    extension<TConsumer>(ConsumerBuilder<TConsumer> builder)
        where TConsumer : class, IConsumer
    {
        public ConsumerBuilder<TConsumer> AzureServiceBusFilter<TMessage>(SqlRuleFilter filter)
        {
            // Store the filter somewhere keyed by consumer and message
            // Then use in topology builder
            
            return builder;
        }
    }
}
