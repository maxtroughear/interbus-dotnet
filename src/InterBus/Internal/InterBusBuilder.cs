using System.Diagnostics.CodeAnalysis;
using InterBus.Middleware;
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
        Func<DispatchContext, IServiceProvider, CancellationToken, Task> dispatch)
        where TConsumer : class, IConsumer =>
        AddConsumer<TConsumer>(messageTypes, RoutingConventions.QueueName(typeof(TConsumer)), dispatch, null);

    /// <inheritdoc/>
    public IInterBusBuilder AddConsumer<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TConsumer>(
        Type[] messageTypes,
        Func<DispatchContext, IServiceProvider, CancellationToken, Task> dispatch,
        Action<IConsumerBuilder>? configure)
        where TConsumer : class, IConsumer =>
        AddConsumer<TConsumer>(messageTypes, RoutingConventions.QueueName(typeof(TConsumer)), dispatch, configure);

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
        Func<DispatchContext, IServiceProvider, CancellationToken, Task> dispatch)
        where TConsumer : class, IConsumer =>
        AddConsumer<TConsumer>(messageTypes, queueName, dispatch, null);

    /// <inheritdoc/>
    public IInterBusBuilder AddConsumer<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TConsumer>(
        Type[] messageTypes,
        string queueName,
        Func<DispatchContext, IServiceProvider, CancellationToken, Task> dispatch,
        Action<IConsumerBuilder>? configure)
        where TConsumer : class, IConsumer
    {
        var descriptor = new ConsumerDescriptor(typeof(TConsumer), queueName, messageTypes, dispatch);
        configure?.Invoke(new ConsumerBuilder(descriptor, Services));
        _options.AddConsumer(descriptor);
        Services.TryAddScoped<TConsumer>();
        return this;
    }

    /// <inheritdoc/>
    public IInterBusBuilder UseSerializer<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TSerializer>()
        where TSerializer : class, IMessageSerializer
    {
        Services.Replace(ServiceDescriptor.Singleton<IMessageSerializer, TSerializer>());
        return this;
    }

    /// <inheritdoc/>
    public IInterBusBuilder UsePublishMiddleware<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TMiddleware>()
        where TMiddleware : class, IPublishMiddleware
    {
        Services.AddScoped<IPublishMiddleware, TMiddleware>();
        return this;
    }

    /// <inheritdoc/>
    public IInterBusBuilder UseConsumeMiddleware<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TMiddleware>()
        where TMiddleware : class, IConsumeMiddleware
    {
        Services.AddScoped<IConsumeMiddleware, TMiddleware>();
        return this;
    }

    /// <inheritdoc/>
    public IInterBusBuilder UseScheduleMiddleware<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TMiddleware>()
        where TMiddleware : class, IScheduleMiddleware
    {
        Services.AddScoped<IScheduleMiddleware, TMiddleware>();
        return this;
    }
}
