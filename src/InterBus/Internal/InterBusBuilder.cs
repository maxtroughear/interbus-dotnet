using System.Diagnostics.CodeAnalysis;
using InterBus.Consuming;
using InterBus.DeadLettering;
using InterBus.Dispatching;
using InterBus.Logging;
using InterBus.Publishing;
using InterBus.Sagas;
using InterBus.Sending;
using InterBus.Telemetry;
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
        AddConsumer<TConsumer>(messageTypes, RoutingConventions.QueueUri(Options.QueueUriScheme, typeof(TConsumer)),
            dispatch, configure);

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

        // Telemetry must be the outermost dispatch and publish middleware.
        consumerBuilder.UseDispatchMiddleware<TelemetryDispatchMiddleware>();
        consumerBuilder.UsePublishMiddleware<TelemetryPublishMiddleware>();

        consumerBuilder.UseConsumeMiddleware<DeadLetterConsumeMiddleware>();
        consumerBuilder.UseConsumeMiddleware<ExceptionLoggingConsumeMiddleware>();

        configure?.Invoke(consumerBuilder);

        Options.AddConsumer(descriptor);
        Services.TryAddScoped<TConsumer>();
        return this;
    }

    /// <inheritdoc/>
    public IInterBusBuilder AddSaga<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TSaga,
        TState>(
        Type[] messageTypes,
        Func<DispatchContext, IServiceProvider, CancellationToken, Task> dispatch,
        Action<ISagaBuilder<TSaga, TState>>? configure = null)
        where TSaga : class, ISaga<TState>
        where TState : class, new()
    {
        var queue = RoutingConventions.QueueUri(Options.QueueUriScheme, typeof(TSaga));
        var descriptor = new ConsumerDescriptor(typeof(TSaga), queue, messageTypes, dispatch);
        var sagaOptions = new SagaOptions<TSaga>();
        var sagaBuilder = new SagaBuilder<TSaga, TState>(descriptor, Services, sagaOptions);

        // Apply telemetry as the outermost dispatch and publish middleware.
        sagaBuilder.UseDispatchMiddleware<TelemetryDispatchMiddleware>();
        sagaBuilder.UsePublishMiddleware<TelemetryPublishMiddleware>();

        // Apply the same default middleware as regular consumers.
        sagaBuilder.UseConsumeMiddleware<DeadLetterConsumeMiddleware>();
        sagaBuilder.UseConsumeMiddleware<ExceptionLoggingConsumeMiddleware>();

        configure?.Invoke(sagaBuilder);

        Services.AddSingleton(sagaOptions);
        Options.AddConsumer(descriptor);
        Services.TryAddScoped<TSaga>();
        Services.TryAddScoped<SagaExecutor<TSaga, TState>>();
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
