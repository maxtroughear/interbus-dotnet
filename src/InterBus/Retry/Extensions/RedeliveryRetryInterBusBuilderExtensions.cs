using InterBus.Middleware;
using InterBus.Retry;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

// ReSharper disable once CheckNamespace
namespace InterBus;

/// <summary>
/// Extension methods for adding transport redelivery retry middleware to the consume pipeline.
/// </summary>
public static class RedeliveryRetryInterBusBuilderExtensions
{
    /// <summary>
    /// Adds <see cref="RedeliveryRetryConsumeMiddleware"/> to the consume pipeline using the supplied
    /// <paramref name="configure"/> delegate to customise <see cref="RedeliveryRetryOptions"/>.
    /// </summary>
    /// <param name="builder">The InterBus builder.</param>
    /// <param name="configure">
    /// Optional delegate to configure the <see cref="RedeliveryRetryOptions"/> instance.
    /// When <c>null</c>, defaults are used (3 retries, no delay, catch all exceptions).
    /// </param>
    /// <returns>The builder for fluent chaining.</returns>
    public static IInterBusBuilder UseRedeliveryRetryMiddleware(
        this IInterBusBuilder builder,
        Action<RedeliveryRetryOptions>? configure = null)
    {
        var options = new RedeliveryRetryOptions();
        configure?.Invoke(options);

        builder.Services.AddSingleton(options);
        builder.Services.AddScoped<IConsumeMiddleware>(sp => new RedeliveryRetryConsumeMiddleware(
            sp.GetRequiredService<RedeliveryRetryOptions>(),
            sp.GetRequiredService<IInterBusTransport>(),
            sp.GetService<IMessageSchedulingProvider>(),
            sp.GetRequiredService<ILogger<RedeliveryRetryConsumeMiddleware>>()));
        return builder;
    }
}
