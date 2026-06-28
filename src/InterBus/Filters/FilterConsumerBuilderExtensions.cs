using InterBus.Consuming;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.Filters;

public static class FilterConsumerBuilderExtensions
{
    extension(ConsumerBuilder builder)
    {
        public ConsumerBuilder Filter<TMessage>(Func<TMessage, bool> filter)
            where TMessage : class
        {
            builder.Services.AddKeyedScoped<IConsumeMiddleware, FilterConsumeMiddleware<TMessage>>(
                builder.Descriptor.ConsumerType,
                (_, _) => new FilterConsumeMiddleware<TMessage>(filter));

            return builder;
        }
    }
}
