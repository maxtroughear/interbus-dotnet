using InterBus.Consuming;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace InterBus.Sagas;

/// <summary>
/// Fluent builder for configuring a registered saga.
/// Extends <see cref="ConsumerBuilder"/> so that all consumer-level options
/// (concurrency, middleware) are available alongside saga-specific settings.
/// </summary>
/// <typeparam name="TSaga">The saga type</typeparam>
/// <typeparam name="TState">The saga state type</typeparam>
public sealed class SagaBuilder<TSaga, TState>(
    ConsumerDescriptor descriptor,
    IServiceCollection services,
    SagaOptions<TSaga> options)
    : ConsumerBuilder<TSaga>(descriptor, services)
    where TSaga : class, ISaga<TState>
    where TState : class, new()
{
    /// <summary>
    /// Sets the time-to-live for saga instances. Repository implementations will attempt to
    /// expire stored state after this duration. The default is no expiry.
    /// </summary>
    public SagaBuilder<TSaga, TState> WithTimeToLive(TimeSpan ttl)
    {
        options.TimeToLive = ttl;
        return this;
    }

    public new SagaBuilder<TSaga, TState> Consumes<TSagaConsumer, TMessage>()
        where TSagaConsumer : class, TSaga, ISagaConsumer<TState, TMessage>
        where TMessage : class
        => Consumes<TSagaConsumer, TMessage>
            (RoutingConventions.MessageTypeString(typeof(TMessage)));

    public new SagaBuilder<TSaga, TState> Consumes<TSagaConsumer, TMessage>(string messageTypeName)
        where TSagaConsumer : class, TSaga, ISagaConsumer<TState, TMessage>
        where TMessage : class
    {
        Descriptor.MessageTypes.Add(typeof(TMessage));
        Descriptor.SetRoute(messageTypeName,
            static (context, pipeline, ct) => pipeline.ExecuteAsync<TSagaConsumer, TMessage>(context, ct));

        Services.TryAddScoped<IConsumerExecutor<TSagaConsumer, TMessage>,
            SagaExecutor<TSagaConsumer, TState, TMessage>>();

        return this;
    }
}
