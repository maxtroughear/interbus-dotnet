using System.Diagnostics.CodeAnalysis;
using InterBus.Consuming;
using InterBus.Dispatching;
using InterBus.Publishing;
using InterBus.Sending;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus;

/// <summary>
/// Fluent builder for configuring a registered consumer.
/// Obtained from
/// <see cref="Extensions.InterBusBuilderExtensions.WithConsumers"/> or via the
/// source-generated <c>AddConsumer&lt;TConsumer&gt;(Action&lt;IConsumerBuilder&gt;)</c> overload.
/// </summary>
public interface IConsumerBuilder
{
    /// <summary>The underlying service collection, available for transport extension methods to register additional services.</summary>
    IServiceCollection Services { get; }

    /// <summary>
    /// Configures the number of concurrent message handlers for this consumer's queue.
    /// Transports that support concurrent dispatch per queue (e.g. RabbitMQ) use this value
    /// to configure the channel's dispatch concurrency and prefetch count.
    /// Defaults to <c>1</c> (sequential dispatch) when not configured.
    /// </summary>
    /// <param name="concurrencyCount">
    /// The number of concurrent message handlers. Must be at least <c>1</c>.
    /// </param>
    IConsumerBuilder WithConcurrency(int concurrencyCount);

    /// <summary>
    /// Add a middleware step to the consume pipeline for this consumer only.
    /// Steps are executed in registration order - the first registered step is outermost
    /// </summary>
    /// <remarks>
    /// The middleware type is automatically registered as a scoped service in the DI container.
    /// </remarks>
    IConsumerBuilder UseConsumeMiddleware<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TMiddleware>()
        where TMiddleware : class, IConsumeMiddleware;

    /// <summary>
    /// Add a middleware step to the send pipeline for this consumer only.
    /// Steps are executed in registration order - the first registered step is outermost
    /// </summary>
    /// <remarks>
    /// The middleware type is automatically registered as a scoped service in the DI container.
    /// </remarks>
    public IConsumerBuilder UseSendMiddleware<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TMiddleware>()
        where TMiddleware : class, ISendMiddleware;

    /// <summary>
    /// Add a middleware step to the publish pipeline for this consumer only.
    /// Steps are executed in registration order - the first registered step is outermost
    /// </summary>
    /// <remarks>
    /// The middleware type is automatically registered as a scoped service in the DI container.
    /// </remarks>
    public IConsumerBuilder UsePublishMiddleware<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TMiddleware>()
        where TMiddleware : class, IPublishMiddleware;

    /// <summary>
    /// Add a middleware step to the dispatch pipeline for this consumer only.
    /// Dispatch middleware runs before the consumer's handler and its consume middleware.
    /// Steps are executed in registration order - the first registered step is outermost.
    /// </summary>
    /// <remarks>
    /// The middleware type is automatically registered as a scoped service in the DI container.
    /// </remarks>
    public IConsumerBuilder UseDispatchMiddleware<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TMiddleware>()
        where TMiddleware : class, IDispatchMiddleware;
}
