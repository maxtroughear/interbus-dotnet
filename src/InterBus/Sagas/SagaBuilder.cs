using System.Diagnostics.CodeAnalysis;
using InterBus.Consuming;
using InterBus.Dispatching;
using InterBus.Publishing;
using InterBus.Sending;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.Sagas;

/// <inheritdoc cref="ISagaBuilder{TSaga, TState}"/>
internal sealed class SagaBuilder<TSaga, TState>(
    ConsumerDescriptor descriptor,
    IServiceCollection services,
    SagaOptions<TSaga> options)
    : ISagaBuilder<TSaga, TState>
    where TSaga : class, ISaga<TState>
    where TState : class, new()
{
    private readonly ConsumerBuilder _inner = new(descriptor, services);

    #region IConsumerBuilder

    /// <inheritdoc/>
    public IServiceCollection Services => services;

    /// <inheritdoc/>
    public Type ConsumerType => _inner.ConsumerType;

    /// <inheritdoc/>
    public IConsumerBuilder WithConcurrency(int concurrencyCount)
    {
        _inner.WithConcurrency(concurrencyCount);
        return this;
    }

    /// <inheritdoc/>
    public IConsumerBuilder UseConsumeMiddleware<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TMiddleware>()
        where TMiddleware : class, IConsumeMiddleware
    {
        _inner.UseConsumeMiddleware<TMiddleware>();
        return this;
    }

    /// <inheritdoc/>
    public IConsumerBuilder UseSendMiddleware<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TMiddleware>()
        where TMiddleware : class, ISendMiddleware
    {
        _inner.UseSendMiddleware<TMiddleware>();
        return this;
    }

    /// <inheritdoc/>
    public IConsumerBuilder UsePublishMiddleware<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TMiddleware>()
        where TMiddleware : class, IPublishMiddleware
    {
        _inner.UsePublishMiddleware<TMiddleware>();
        return this;
    }

    /// <inheritdoc/>
    public IConsumerBuilder UseDispatchMiddleware<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TMiddleware>()
        where TMiddleware : class, IDispatchMiddleware
    {
        _inner.UseDispatchMiddleware<TMiddleware>();
        return this;
    }

    #endregion

    /// <inheritdoc/>
    public ISagaBuilder<TSaga, TState> WithTimeToLive(TimeSpan ttl)
    {
        options.TimeToLive = ttl;
        return this;
    }
}
