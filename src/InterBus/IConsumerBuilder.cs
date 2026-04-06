using System.Diagnostics.CodeAnalysis;
using InterBus.Middleware;

namespace InterBus;

/// <summary>
/// Fluent builder for configuring a registered consumer.
/// Obtained from
/// <see cref="Extensions.InterBusBuilderExtensions.WithConsumers"/> or via the
/// source-generated <c>AddConsumer&lt;TConsumer&gt;(Action&lt;IConsumerBuilder&gt;)</c> overload.
/// </summary>
public interface IConsumerBuilder
{
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
    /// Per-consumer middleware runs after any globally-registered consume middleware,
    /// closer to the consumer. Steps are executed in registration order - the first
    /// registered step is outermost within the per-consumer section.
    /// </summary>
    /// <remarks>
    /// The middleware type is automatically registered as a scoped service in the DI container.
    /// </remarks>
    IConsumerBuilder UseConsumeMiddleware<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TMiddleware>()
        where TMiddleware : class, IConsumeMiddleware;
}
