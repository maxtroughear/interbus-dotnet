using Azure.Messaging.ServiceBus.Administration;
using InterBus.Consuming;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.AzureServiceBus.Extensions;

public static class AzureServiceBusConsumerBuilderExtensions
{
    extension<TConsumer>(ConsumerBuilder<TConsumer> builder)
        where TConsumer : class, IConsumer
    {
        public ConsumerBuilder<TConsumer> AzureSqlRuleFilter<TMessage>(SqlRuleFilter filter)
        {
            // Store the filter somewhere keyed by consumer and message
            // Then use in topology builder.

            builder.Services.Configure<AzureServiceBusFilterOptions>(options =>
                options.Filters.Add((typeof(TConsumer), typeof(TMessage)), filter));

            return builder;
        }
    }
}
