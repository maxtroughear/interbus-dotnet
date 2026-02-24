using System.Reflection;
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

    public IInterBusBuilder AddConsumer<TConsumer>() where TConsumer : class, IConsumer
    {
        // Discover all IConsumer<T> interfaces implemented by TConsumer (startup-time reflection only).
        var consumerInterfaces = typeof(TConsumer)
            .GetInterfaces()
            .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IConsumer<>));

        foreach (var iface in consumerInterfaces)
        {
            var messageType = iface.GetGenericArguments()[0];

            // Build the ConsumerDescriptor using a generic helper to capture TConsumer and T at compile time,
            // so the dispatch delegate itself is free of runtime reflection.
            var descriptor = (ConsumerDescriptor)typeof(InterBusBuilder)
                .GetMethod(nameof(CreateDescriptor), BindingFlags.Static | BindingFlags.NonPublic)!
                .MakeGenericMethod(typeof(TConsumer), messageType)
                .Invoke(null, null)!;

            _options.AddConsumer(descriptor);
        }

        // Register the consumer as scoped so it is resolved per-message via a DI scope.
        Services.AddScoped<TConsumer>();
        return this;
    }

    public IInterBusBuilder AddConsumer<TConsumer, T>()
        where TConsumer : class, IConsumer<T>
        where T : class
    {
        // Both type parameters are known at compile time — no reflection needed.
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
            static (sp, payload, ct) =>
            {
                var consumer = sp.GetRequiredService<TConsumer>();
                var message = JsonSerializer.Deserialize<T>(payload)
                    ?? throw new InvalidOperationException(
                        $"Failed to deserialize message payload as {typeof(T).FullName}.");
                return consumer.Consume(message, ct);
            });
    }
}
