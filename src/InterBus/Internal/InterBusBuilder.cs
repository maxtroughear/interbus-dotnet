using System.Diagnostics.CodeAnalysis;
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

        _options.AddConsumer(new ConsumerDescriptor(typeof(TConsumer), queueName, topicNames, dispatch));
        Services.TryAddScoped<TConsumer>();
        return this;
    }
}
