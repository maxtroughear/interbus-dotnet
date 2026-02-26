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
    /// Called by source-generated <c>AddConsumers()</c>. The <paramref name="dispatch"/>
    /// delegate is generated at compile time and routes using the <see cref="InterBusHeaders.MessageType"/>
    /// header
    /// </remarks>
    public IInterBusBuilder AddConsumer<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TConsumer>(
        Type[] messageTypes,
        Func<IServiceProvider, string, IConsumeContext, CancellationToken, Task> dispatch)
        where TConsumer : class, IConsumer =>
        AddConsumer<TConsumer>(messageTypes, RoutingConventions.QueueName(typeof(TConsumer)), dispatch);
    
    /// <inheritdoc/>
    /// <remarks>
    /// Called by source-generated <c>AddConsumers()</c>. The <paramref name="dispatch"/>
    /// delegate is generated at compile time and routes using the <see cref="InterBusHeaders.MessageType"/>
    /// header
    /// </remarks>
    public IInterBusBuilder AddConsumer<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TConsumer>(
        Type[] messageTypes,
        string queueName,
        Func<IServiceProvider, string, IConsumeContext, CancellationToken, Task> dispatch)
        where TConsumer : class, IConsumer
    {
        _options.AddConsumer(new ConsumerDescriptor(typeof(TConsumer), queueName, messageTypes, dispatch));
        Services.TryAddScoped<TConsumer>();
        return this;
    }

    /// <inheritdoc/>
    public IInterBusBuilder AddPublishMiddleware<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TMiddleware>()
        where TMiddleware : class, IPublishMiddleware
    {
        Services.AddScoped<IPublishMiddleware, TMiddleware>();
        return this;
    }

    /// <inheritdoc/>
    public IInterBusBuilder AddConsumeMiddleware<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TMiddleware>()
        where TMiddleware : class, IConsumeMiddleware
    {
        Services.AddScoped<IConsumeMiddleware, TMiddleware>();
        return this;
    }
}
