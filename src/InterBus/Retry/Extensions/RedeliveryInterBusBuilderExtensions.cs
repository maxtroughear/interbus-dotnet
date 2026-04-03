using InterBus.Middleware;
using InterBus.Retry;
using Microsoft.Extensions.DependencyInjection;

// ReSharper disable once CheckNamespace
namespace InterBus;

/// <summary>
/// Extension methods for adding transport redelivery retry middleware to the consume pipeline.
/// </summary>
public static class RedeliveryInterBusBuilderExtensions
{
    /// <summary>
    /// Adds <see cref="RedeliverySchedulingConsumeMiddleware"/> to the consume pipeline using the supplied
    /// <paramref name="configure"/> delegate to customise <see cref="RedeliveryOptions"/>.
    /// </summary>
    /// <param name="builder">The InterBus builder.</param>
    /// <param name="configure">
    /// Optional delegate to configure the <see cref="RedeliveryOptions"/> instance.
    /// When <c>null</c>, defaults are used (3 retries, no delay, catch all exceptions).
    /// </param>
    /// <returns>The builder for fluent chaining.</returns>
    /// <remarks>
    /// Requires an <see cref="IMessageSchedulingProvider"/> to be registered first, which is used to schedule the
    /// redelivery of messages when retries are needed
    /// </remarks>
    public static IInterBusBuilder UseRedeliverySchedulingMiddleware(
        this IInterBusBuilder builder,
        Action<RedeliveryOptions>? configure = null)
    {
        var options = new RedeliveryOptions();
        configure?.Invoke(options);

        builder.Services.AddSingleton(options);
        builder.Services.AddScoped<IConsumeMiddleware, RedeliverySchedulingConsumeMiddleware>();

        return builder;
    }

    /// <summary>
    /// Adds <see cref="RedeliveryDelayConsumeMiddleware"/> to the consume pipeline using the supplied
    /// <paramref name="configure"/> delegate to customise <see cref="RedeliveryOptions"/>.
    /// </summary>
    /// <param name="builder">The InterBus builder.</param>
    /// <param name="configure">
    /// Optional delegate to configure the <see cref="RedeliveryOptions"/> instance.
    /// When <c>null</c>, defaults are used (3 retries, no delay, catch all exceptions).
    /// </param>
    /// <returns>The builder for fluent chaining.</returns>
    /// <remarks>
    /// Uses the transport's native delay mechanism (<see cref="TransportSendContext.Delay"/>) to defer
    /// redelivery, without requiring a separate scheduling infrastructure.
    /// Transports that do not support deferred visibility will deliver the message immediately.
    /// </remarks>
    public static IInterBusBuilder UseRedeliveryTransportDelayMiddleware(
        this IInterBusBuilder builder,
        Action<RedeliveryOptions>? configure = null)
    {
        var options = new RedeliveryOptions();
        configure?.Invoke(options);

        builder.Services.AddSingleton(options);
        builder.Services.AddScoped<IConsumeMiddleware, RedeliveryDelayConsumeMiddleware>();

        return builder;
    }

    /// <summary>
    /// Adds <see cref="RedeliveryImmediateConsumeMiddleware"/> to the consume pipeline using the supplied
    /// <paramref name="configure"/> delegate to customise <see cref="RedeliveryOptions"/>
    /// </summary>
    /// <param name="builder">The InterBus builder</param>
    /// <param name="configure">
    /// Optional delegate to configure the <see cref="RedeliveryOptions"/> instance.
    /// When <c>null</c>, defaults are used (3 retries, no delay, catch all exceptions)
    /// </param>
    /// <returns>The builder for fluent chaining.</returns>
    /// <remarks>
    /// Does not support delays
    /// </remarks>
    public static IInterBusBuilder UseRedeliveryImmediateMiddleware(
        this IInterBusBuilder builder,
        Action<RedeliveryOptions>? configure = null)
    {
        var options = new RedeliveryOptions();
        configure?.Invoke(options);

        builder.Services.AddSingleton(options);
        builder.Services.AddScoped<IConsumeMiddleware, RedeliveryImmediateConsumeMiddleware>();

        return builder;
    }
}
