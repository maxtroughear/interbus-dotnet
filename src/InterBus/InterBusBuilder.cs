using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using InterBus.Consuming;
using InterBus.DeadLettering;
using InterBus.Dispatching;
using InterBus.Internal;
using InterBus.Logging;
using InterBus.Publishing;
using InterBus.Sagas;
using InterBus.Scheduling;
using InterBus.Sending;
using InterBus.Serialization;
using InterBus.Telemetry;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace InterBus;

/// <summary>
/// Fluent builder for configuring InterBus - consumers, transport, and options.
/// Obtained from <c>services.AddInterBus(builder => { ... })</c>.
/// </summary>
public sealed class InterBusBuilder
{
    public IServiceCollection Services { get; }

    public InterBusOptions Options { get; }

    internal InterBusBuilder(IServiceCollection services, InterBusOptions options)
    {
        Services = services;
        Options = options;
    }

    /// <summary>
    /// Configure an explicit topic name for message type <typeparamref name="T"/>. The name is used
    /// by both consumer registration and <c>IMessagePublisher.PublishAsync&lt;T&gt;</c>, overriding the
    /// convention-based name derived from the type's fully-qualified name.
    /// Call this before <c>AddConsumer&lt;TConsumer&gt;</c> for the same message type.
    /// </summary>
    public InterBusBuilder ConfigureTopic<T>(string topicName) where T : class
    {
        Options.SetTopicName(typeof(T), topicName);
        return this;
    }

    /// <summary>
    /// Register a consumer with a source-generated dispatch delegate and an optional
    /// consumer-level configuration delegate.
    /// The <paramref name="configure"/> delegate is invoked immediately after registration,
    /// allowing per-consumer options such as concurrent dispatch to be set inline.
    /// </summary>
    public InterBusBuilder AddConsumer<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TConsumer>(
        Func<DispatchContext, ConsumePipelineExecutor, IServiceProvider, CancellationToken, Task> dispatch,
        Action<ConsumerBuilder<TConsumer>>? configure = null)
        where TConsumer : class, IConsumer
    {
        var queue = RoutingConventions.QueueUri(Options.QueueUriScheme, typeof(TConsumer));
        
        var descriptor = new ConsumerDescriptor(typeof(TConsumer), queue, dispatch);
        var consumerBuilder = new ConsumerBuilder<TConsumer>(descriptor, Services);

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

    /// <summary>
    /// Applies a consumer builder configuration delegate to <em>all previously registered</em>
    /// consumers. Useful after <c>AddConsumers()</c> to configure options (such as
    /// <see cref="ConsumerDescriptor.ConcurrencyCount"/>) for every consumer at once.
    /// <para>
    /// Only consumers already registered at the time of this call are affected. Consumers
    /// registered afterwards are not configured by this call.
    /// </para>
    /// </summary>
    /// <param name="configure">
    /// Delegate that receives an <see cref="ConsumerBuilder"/> for each registered consumer
    /// and applies the desired configuration.
    /// </param>
    public InterBusBuilder WithConsumers(Action<ConsumerBuilder> configure)
    {
        foreach (var descriptor in Options.Consumers)
            configure(new ConsumerBuilder(descriptor, Services));

        return this;
    }

    /// <summary>
    /// Register a saga with a source-generated dispatch delegate.
    /// Called by the source-generated <c>AddSaga&lt;TSaga, TState&gt;(configure)</c> extension
    /// method after it looks up the dispatch delegate from the per-assembly dispatch module.
    /// </summary>
    /// <remarks>
    /// The <paramref name="configure"/> delegate is invoked immediately after registration.
    /// Use it to configure the saga repository (e.g. <c>saga.UseInMemoryRepository()</c>),
    /// time-to-live, concurrency, or additional middleware.
    /// </remarks>
    public InterBusBuilder AddSaga<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TSaga,
        TState>(
        Func<DispatchContext, ConsumePipelineExecutor, IServiceProvider, CancellationToken, Task> dispatch,
        Action<SagaBuilder<TSaga, TState>>? configure = null)
        where TSaga : class, ISaga<TState>
        where TState : class, new()
    {
        var queue = RoutingConventions.QueueUri(Options.QueueUriScheme, typeof(TSaga));
        var descriptor = new ConsumerDescriptor(typeof(TSaga), queue, dispatch);
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
        return this;
    }

    /// <summary>
    /// Register the InterBus scheduling infrastructure. This enables injecting
    /// <see cref="IScheduler"/> to schedule messages for deferred delivery.
    /// <para>
    /// A concrete <see cref="IMessageSchedulingProvider"/> must also be registered (e.g. via
    /// <c>AddTickerQScheduler()</c>) to supply the underlying delivery mechanism.
    /// </para>
    /// </summary>
    public InterBusBuilder AddScheduling(Action<SchedulerBuilder> configure)
    {
        Services.AddScoped<IScheduler, Scheduler>();

        configure(new SchedulerBuilder(this, Services));

        return this;
    }

    /// <summary>
    /// Register a <see cref="SystemTextJsonMessageSerializer"/> configured with the provided
    /// <paramref name="options"/>, replacing the default reflection-based serialiser.
    /// <para>
    /// Designed for trimmed or Native AOT applications where a source-generated
    /// <see cref="System.Text.Json.Serialization.JsonSerializerContext"/> must be supplied:
    /// </para>
    /// <code>
    /// bus.UseSystemTextJsonSerializer(MyAotJsonContext.Default.Options);
    /// </code>
    /// </summary>
    /// <param name="options">
    /// <see cref="JsonSerializerOptions"/> to pass to <see cref="SystemTextJsonMessageSerializer"/>.
    /// Typically <c>MyJsonContext.Default.Options</c> from a source-generated
    /// <see cref="System.Text.Json.Serialization.JsonSerializerContext"/>.
    /// </param>
    public InterBusBuilder UseSystemTextJsonSerializer(JsonSerializerOptions options)
    {
        Services.Replace(ServiceDescriptor.Singleton<IMessageSerializer>(new SystemTextJsonMessageSerializer(options)));
        return this;
    }

    /// <summary>
    /// Register a custom <see cref="IMessageSerializer"/> implementation.
    /// The type is registered as a singleton in the DI container, replacing the default
    /// <see cref="SystemTextJsonMessageSerializer"/>.
    /// </summary>
    /// <typeparam name="TSerializer">The serialiser type to register.</typeparam>
    public InterBusBuilder UseSerializer<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TSerializer>()
        where TSerializer : class, IMessageSerializer
    {
        Services.Replace(ServiceDescriptor.Singleton<IMessageSerializer, TSerializer>());
        return this;
    }

    /// <summary>
    /// Add a middleware step to the publish pipeline, used in the bus context.
    /// Steps are executed in registration order - the first registered step is outermost
    /// and runs first. The terminal step serializes the message and starts the send pipeline.
    /// </summary>
    /// <remarks>
    /// Use <see cref="ConsumerBuilder.UsePublishMiddleware{TMiddleware}"/> to add publish
    /// middleware to the consume context
    /// </remarks>
    public InterBusBuilder UsePublishMiddleware<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TMiddleware>()
        where TMiddleware : class, IPublishMiddleware
    {
        // Keyed to bus
        Services.AddKeyedScoped<IPublishMiddleware, TMiddleware>(InterBusConstants.BusContext);
        return this;
    }

    /// <summary>
    /// Add a middleware step to the send pipeline, used in the bus context.
    /// Steps are executed in registration order - the first registered step is outermost
    /// and runs first. The terminal step calls the configured <see cref="ITransport"/>.
    /// </summary>
    /// <remarks>
    /// Use <see cref="ConsumerBuilder.UseSendMiddleware{TMiddleware}"/> to add send
    /// middleware to the consume context
    /// </remarks>
    public InterBusBuilder UseSendMiddleware<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TMiddleware>()
        where TMiddleware : class, ISendMiddleware
    {
        // Keyed to bus
        Services.AddKeyedScoped<ISendMiddleware, TMiddleware>(InterBusConstants.BusContext);
        return this;
    }
}
