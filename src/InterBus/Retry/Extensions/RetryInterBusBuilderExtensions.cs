using InterBus.Consuming;
using InterBus.Retry;
using Microsoft.Extensions.DependencyInjection;

// ReSharper disable once CheckNamespace
namespace InterBus;

/// <summary>
/// Extension methods for adding retry middleware to the InterBus consume pipeline.
/// </summary>
public static class RetryInterBusBuilderExtensions
{
    /// <summary>
    /// Adds <see cref="RetryConsumeMiddleware"/> to the consume pipeline using the supplied
    /// <paramref name="configure"/> delegate to customise <see cref="RetryOptions"/>.
    /// </summary>
    /// <param name="builder">The InterBus builder.</param>
    /// <param name="configure">
    /// Optional delegate to configure the <see cref="RetryOptions"/> instance.
    /// When <c>null</c>, the defaults from <see cref="RetryOptions"/> are used
    /// (3 retries, no delay, catch all exceptions).
    /// </param>
    /// <returns>The builder for fluent chaining.</returns>
    public static ConsumerBuilder UseRetryMiddleware(
        this ConsumerBuilder builder,
        Action<RetryOptions>? configure = null)
    {
        var options = new RetryOptions();
        configure?.Invoke(options);

        builder.Services.AddSingleton(options);

        return builder.UseConsumeMiddleware<RetryConsumeMiddleware>();
    }
}
