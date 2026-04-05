namespace InterBus.RabbitMQ;

/// <summary>
/// Per-queue RabbitMQ options. This type is no longer used; per-consumer concurrency is now
/// configured via
/// <see cref="InterBus.Extensions.InterBusBuilderExtensions.WithConsumerConcurrency{TConsumer}"/>
/// and stored on <see cref="ConsumerDescriptor.ConcurrencyCount"/>.
/// </summary>
[Obsolete("Use bus.WithConsumerConcurrency<TConsumer>(count) on IInterBusBuilder instead.")]
public sealed class RabbitMqQueueOptions;
