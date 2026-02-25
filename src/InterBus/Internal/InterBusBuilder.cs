using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
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

    public IInterBusBuilder AddConsumer<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TConsumer, T>()
        where TConsumer : class, IConsumer<T>
        where T : class
    {
        _options.AddConsumer(CreateDescriptor<TConsumer, T>(_options.SerializerOptions));
        Services.AddScoped<TConsumer>();
        return this;
    }

    private static ConsumerDescriptor CreateDescriptor<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TConsumer, T>(JsonSerializerOptions serializerOptions)
        where TConsumer : class, IConsumer<T>
        where T : class
    {
        var topicName = RoutingConventions.TopicName(typeof(T));
        var queueName = RoutingConventions.QueueName(typeof(TConsumer), typeof(T));

        // Resolve JsonTypeInfo<T> at registration time using the configured serializer options.
        // Using the typed overload avoids runtime reflection during message dispatch.
        JsonTypeInfo<T> typeInfo = (JsonTypeInfo<T>)serializerOptions.GetTypeInfo(typeof(T));

        return new ConsumerDescriptor(
            typeof(TConsumer),
            typeof(T),
            topicName,
            queueName,
            (sp, payload, headers, ct) =>
            {
                var context = new ConsumeContext(headers);
                var consumer = sp.GetRequiredService<TConsumer>();
                var message = JsonSerializer.Deserialize(payload, typeInfo)
                              ?? throw new InvalidOperationException(
                                  $"Failed to deserialize message payload as {typeof(T).FullName}.");
                return consumer.Consume(message, context, ct);
            });
    }
}
