using System.Diagnostics.CodeAnalysis;
using InterBus.Middleware;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace InterBus.Internal;

/// <inheritdoc/>
internal sealed class ConsumerBuilder(ConsumerDescriptor descriptor, IServiceCollection services) : IConsumerBuilder
{
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
    public IConsumerBuilder UseConsumeMiddleware<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TMiddleware>()
        where TMiddleware : class, IConsumeMiddleware
    {
        descriptor.ConsumeMiddlewareTypes.Add(typeof(TMiddleware));
        services.TryAddScoped<TMiddleware>();
        return this;
    }
}
