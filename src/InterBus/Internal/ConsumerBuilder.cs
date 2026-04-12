using System.Diagnostics.CodeAnalysis;
using InterBus.Middleware;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace InterBus.Internal;

/// <inheritdoc/>
internal sealed class ConsumerBuilder(ConsumerDescriptor descriptor, IServiceCollection services) : IConsumerBuilder
{
    public IServiceCollection Services => services;

    /// <inheritdoc/>
    public IConsumerBuilder WithConcurrency(int concurrencyCount)
    {
        if (concurrencyCount < 1)
            throw new ArgumentOutOfRangeException(
                nameof(concurrencyCount), "Concurrency count must be at least 1.");

        descriptor.ConcurrencyCount = concurrencyCount;
        return this;
    }

    /// <inheritdoc/>
    public IConsumerBuilder UseConsumeMiddleware(
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] Type middlewareType)
    {
        services.AddKeyedScoped(typeof(IConsumeMiddleware), descriptor.ConsumerType, middlewareType);
        return this;
    }

    /// <inheritdoc/>
    public IConsumerBuilder UseConsumeMiddleware<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TMiddleware>()
        where TMiddleware : class, IConsumeMiddleware
    {
        services.AddKeyedScoped<IConsumeMiddleware, TMiddleware>(descriptor.ConsumerType);
        return this;
    }

    /// <inheritdoc/>
    public IConsumerBuilder UseSendMiddleware<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TMiddleware>()
        where TMiddleware : class, ISendMiddleware
    {
        services.AddKeyedScoped<ISendMiddleware, TMiddleware>(descriptor.ConsumerType);
        return this;
    }

    /// <inheritdoc/>
    public IConsumerBuilder UsePublishMiddleware(
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] Type middlewareType)
    {
        services.AddKeyedScoped(typeof(IPublishMiddleware), descriptor.ConsumerType, middlewareType);
        return this;
    }

    /// <inheritdoc/>
    public IConsumerBuilder UsePublishMiddleware<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TMiddleware>()
        where TMiddleware : class, IPublishMiddleware
    {
        services.AddKeyedScoped<IPublishMiddleware, TMiddleware>(descriptor.ConsumerType);
        return this;
    }

    /// <inheritdoc/>
    public IConsumerBuilder UseScheduleMiddleware<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TMiddleware>()
        where TMiddleware : class, IScheduleMiddleware
    {
        services.AddKeyedScoped<IScheduleMiddleware, TMiddleware>(descriptor.ConsumerType);
        return this;
    }
}
