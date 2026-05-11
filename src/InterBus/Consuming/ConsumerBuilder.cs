using System.Diagnostics.CodeAnalysis;
using InterBus.Dispatching;
using InterBus.Publishing;
using InterBus.Sending;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace InterBus.Consuming;

public class ConsumerBuilder<TConsumer>(ConsumerDescriptor descriptor, IServiceCollection services)
    : ConsumerBuilder(descriptor, services)
    where TConsumer : class, IConsumer
{
    public ConsumerBuilder<TConsumer> Consumes<TMessageConsumer, TMessage>()
        where TMessageConsumer : TConsumer, IConsumer<TMessage>
        where TMessage : class
    {
        Descriptor.MessageTypes.Add(typeof(TMessage));

        Services.TryAddScoped<IConsumerExecutor<TMessageConsumer, TMessage>,
                ConsumerExecutor<TMessageConsumer, TMessage>>();
        
        return this;
    }

    public ConsumerBuilder<TConsumer> Dispatch(Func<DispatchContext, ConsumePipelineExecutor, IServiceProvider, CancellationToken, Task> dispatch)
    {
        Descriptor.Dispatch = dispatch;
        return this;
    }
}

/// <summary>
/// Fluent builder for configuring a registered consumer.
/// Obtained from
/// <see cref="InterBusBuilder.WithConsumers"/> or via the
/// source-generated <c>AddConsumer&lt;TConsumer&gt;(Action&lt;ConsumerBuilder&gt;)</c> overload.
/// </summary>
public class ConsumerBuilder(ConsumerDescriptor descriptor, IServiceCollection services)
{
    /// <summary>
    /// The underlying service collection, available for transport extension methods to register additional services
    /// </summary>
    public IServiceCollection Services => services;

    /// <summary>
    /// The descriptor for the consumer being configured
    /// </summary>
    public ConsumerDescriptor Descriptor => descriptor;
    
    /// <summary>
    /// Configures the number of concurrent message handlers for this consumer's queue.
    /// Transports that support concurrent dispatch per queue (e.g. RabbitMQ) use this value
    /// to configure the channel's dispatch concurrency and prefetch count.
    /// Defaults to <c>1</c> (sequential dispatch) when not configured.
    /// </summary>
    /// <param name="concurrencyCount">
    /// The number of concurrent message handlers. Must be at least <c>1</c>.
    /// </param>
    public ConsumerBuilder WithConcurrency(int concurrencyCount)
    {
        if (concurrencyCount < 1)
            throw new ArgumentOutOfRangeException(
                nameof(concurrencyCount), "Concurrency count must be at least 1");

        descriptor.ConcurrencyCount = concurrencyCount;
        return this;
    }

    /// <summary>
    /// Add a middleware step to the consume pipeline for this consumer only.
    /// Steps are executed in registration order - the first registered step is outermost
    /// </summary>
    /// <remarks>
    /// The middleware type is automatically registered as a scoped service in the DI container.
    /// </remarks>
    public ConsumerBuilder UseConsumeMiddleware<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TMiddleware>()
        where TMiddleware : class, IConsumeMiddleware
    {
        services.AddKeyedScoped<IConsumeMiddleware, TMiddleware>(descriptor.ConsumerType);
        return this;
    }

    /// <summary>
    /// Add a middleware step to the send pipeline for this consumer only.
    /// Steps are executed in registration order - the first registered step is outermost
    /// </summary>
    /// <remarks>
    /// The middleware type is automatically registered as a scoped service in the DI container.
    /// </remarks>
    public ConsumerBuilder UseSendMiddleware<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TMiddleware>()
        where TMiddleware : class, ISendMiddleware
    {
        services.AddKeyedScoped<ISendMiddleware, TMiddleware>(descriptor.ConsumerType);
        return this;
    }

    /// <summary>
    /// Add a middleware step to the publish pipeline for this consumer only.
    /// Steps are executed in registration order - the first registered step is outermost
    /// </summary>
    /// <remarks>
    /// The middleware type is automatically registered as a scoped service in the DI container.
    /// </remarks>
    public ConsumerBuilder UsePublishMiddleware<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TMiddleware>()
        where TMiddleware : class, IPublishMiddleware
    {
        services.AddKeyedScoped<IPublishMiddleware, TMiddleware>(descriptor.ConsumerType);
        return this;
    }

    /// <summary>
    /// Add a middleware step to the dispatch pipeline for this consumer only.
    /// Dispatch middleware runs before the consumer's handler and its consume middleware.
    /// Steps are executed in registration order - the first registered step is outermost.
    /// </summary>
    /// <remarks>
    /// The middleware type is automatically registered as a scoped service in the DI container.
    /// </remarks>
    public ConsumerBuilder UseDispatchMiddleware<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TMiddleware>()
        where TMiddleware : class, IDispatchMiddleware
    {
        services.AddKeyedScoped<IDispatchMiddleware, TMiddleware>(descriptor.ConsumerType);
        return this;
    }
    
    /// <summary>
    /// Registers a consumer-specific serializer resolved by keyed DI using this consumer
    /// type as the key.
    /// </summary>
    public ConsumerBuilder UseMessageSerializer<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TSerializer>()
        where TSerializer : class, IMessageSerializer
    {
        services.AddKeyedSingleton<IMessageSerializer, TSerializer>(descriptor.ConsumerType);

        return this;
    }
}
