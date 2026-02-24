using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.Internal;

internal sealed class InterBusBuilder : IInterBusBuilder
{
    private readonly InterBusOptions _options;

    public IServiceCollection Services { get; }

    internal InterBusBuilder(IServiceCollection services, InterBusOptions options)
    {
        Services = services;
        _options = options;
    }

    public IInterBusBuilder AddConsumer<TConsumer, T>()
        where TConsumer : class, IConsumer<T>
        where T : class
    {
        _options.AddConsumer(CreateDescriptor<TConsumer, T>());
        Services.AddScoped<TConsumer>();
        return this;
    }

    private static ConsumerDescriptor CreateDescriptor<TConsumer, T>()
        where TConsumer : class, IConsumer<T>
        where T : class
    {
        var topicName = RoutingConventions.TopicName(typeof(T));
        var queueName = RoutingConventions.QueueName(typeof(TConsumer), typeof(T));

        return new ConsumerDescriptor(
            typeof(TConsumer),
            typeof(T),
            topicName,
            queueName,
            static (sp, payload, headers, ct) =>
            {
                var context = new MessageContext(headers);
                var accessor = sp.GetRequiredService<IMessageContextAccessor>();
                accessor.MessageContext = context;
                var consumer = sp.GetRequiredService<TConsumer>();
                var message = JsonSerializer.Deserialize<T>(payload)
                              ?? throw new InvalidOperationException(
                                  $"Failed to deserialize message payload as {typeof(T).FullName}.");
                return consumer.Consume(message, context, ct);
            });
    }
}
