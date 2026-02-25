using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace InterBus.Internal;

internal sealed class InterBusBuilder : IInterBusBuilder
{
    private readonly InterBusOptions _options;

    public IServiceCollection Services { get; }

    public InterBusOptions Options => _options;

    internal InterBusBuilder(IServiceCollection services, InterBusOptions options)
    {
        Services = services;
        _options = options;
    }

    public IInterBusBuilder ConfigureTopic<T>(string topicName) where T : class
    {
        _options.SetTopicName(typeof(T), topicName);
        return this;
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Called by source-generated <c>AddGeneratedConsumers()</c>. The <paramref name="dispatch"/>
    /// delegate is generated at compile time and routes using the <see cref="InterBusHeaders.MessageType"/>
    /// header — no runtime reflection.
    /// </remarks>
    public IInterBusBuilder AddConsumer<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TConsumer>(
        Type[] messageTypes,
        Func<IServiceProvider, string, MessageHeaders, CancellationToken, Task> dispatch)
        where TConsumer : class, IConsumer
    {
        var queueName = RoutingConventions.QueueName(typeof(TConsumer));
        var topicNames = messageTypes
            .Select(t => _options.TryGetTopicName(t, out var custom) ? custom : RoutingConventions.TopicName(t))
            .ToArray();

        _options.SetConsumerDispatch(typeof(TConsumer), queueName, topicNames, dispatch);
        Services.TryAddScoped<TConsumer>();
        return this;
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Suitable for manual registration in tests or single-type consumers without source generation.
    /// Calling this multiple times for the same <typeparamref name="TConsumer"/> with different
    /// message types is supported; the handlers are merged into a single queue.
    /// </remarks>
    public IInterBusBuilder AddConsumer<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TConsumer, T>()
        where TConsumer : class, IConsumer<T>
        where T : class
    {
        var topicName = _options.TryGetTopicName(typeof(T), out var customName)
            ? customName
            : RoutingConventions.TopicName(typeof(T));
        var queueName = RoutingConventions.QueueName(typeof(TConsumer));
        var typeInfo = (JsonTypeInfo<T>)_options.SerializerOptions.GetTypeInfo(typeof(T));

        _options.AccumulateConsumerHandler(
            typeof(TConsumer),
            queueName,
            topicName,
            typeof(T).FullName ?? typeof(T).Name,
            (sp, payload, headers, ct) =>
            {
                var context = new ConsumeContext(headers);
                var consumer = sp.GetRequiredService<TConsumer>();
                var message = JsonSerializer.Deserialize(payload, typeInfo)
                              ?? throw new InvalidOperationException(
                                  $"Failed to deserialize message payload as {typeof(T).FullName}.");
                return consumer.Consume(message, context, ct);
            });

        Services.TryAddScoped<TConsumer>();
        return this;
    }

    /// <summary>
    /// Finalizes all accumulated consumer registrations into <see cref="InterBusOptions.Consumers"/>.
    /// Called by <see cref="Extensions.ServiceCollectionExtensions.AddInterBus"/> after the configure
    /// delegate returns.
    /// </summary>
    internal void FinalizeConsumers() => _options.FinalizeConsumers();
}
