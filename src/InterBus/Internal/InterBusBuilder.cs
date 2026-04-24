using System.Diagnostics.CodeAnalysis;
using InterBus.Consumer;
using InterBus.DeadLettering;
using InterBus.Dispatch;
using InterBus.Logging;
using InterBus.Publisher;
using InterBus.Send;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace InterBus.Internal;

internal sealed class InterBusBuilder : IInterBusBuilder
{
    public IServiceCollection Services { get; }

    public InterBusOptions Options { get; }

    internal InterBusBuilder(IServiceCollection services, InterBusOptions options)
    {
        Services = services;
        Options = options;
    }

    public IInterBusBuilder ConfigureTopic<T>(string topicName) where T : class
    {
        Options.SetTopicName(typeof(T), topicName);
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
        AddConsumer<TConsumer>(messageTypes, RoutingConventions.QueueUri(Options.QueueUriScheme, typeof(TConsumer)),
            dispatch, null);

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
        Func<DispatchContext, IServiceProvider, CancellationToken, Task> dispatch,
        Action<IConsumerBuilder>? configure)
        where TConsumer : class, IConsumer =>
        AddConsumer<TConsumer>(messageTypes, RoutingConventions.QueueUri(Options.QueueUriScheme, typeof(TConsumer)), dispatch, configure);
    
    /// <inheritdoc/>
    public IInterBusBuilder AddConsumer<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TConsumer>(
        Type[] messageTypes,
        Uri queue,
        Func<DispatchContext, IServiceProvider, CancellationToken, Task> dispatch,
        Action<IConsumerBuilder>? configure)
        where TConsumer : class, IConsumer
    {
        var descriptor = new ConsumerDescriptor(typeof(TConsumer), queue, messageTypes, dispatch);
        var consumerBuilder = new ConsumerBuilder(descriptor, Services);

        consumerBuilder.UseConsumeMiddleware<DeadLetterConsumeMiddleware>();
        consumerBuilder.UseConsumeMiddleware<ExceptionLoggingConsumeMiddleware>();

        configure?.Invoke(consumerBuilder);

        Options.AddConsumer(descriptor);
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
        // Keyed to bus
        Services.AddKeyedScoped<IPublishMiddleware, TMiddleware>(InterBusConstants.BusContext);
        return this;
    }

    /// <inheritdoc/>
    public IInterBusBuilder UseSendMiddleware<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TMiddleware>()
        where TMiddleware : class, ISendMiddleware
    {
        // Keyed to bus
        Services.AddKeyedScoped<ISendMiddleware, TMiddleware>(InterBusConstants.BusContext);
        return this;
    }
}
