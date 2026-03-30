using InterBus.TickerQ.Internal;
using Microsoft.Extensions.DependencyInjection;
using TickerQ.DependencyInjection;
using TickerQ.Utilities;
using TickerQ.Utilities.Entities;
using TickerQ.Utilities.Enums;

namespace InterBus.TickerQ.Extensions;

/// <summary>Extension methods for configuring the TickerQ scheduler on an <see cref="IInterBusBuilder"/>.</summary>
public static class TickerQSchedulerBuilderExtensions
{
    /// <summary>
    /// Register the TickerQ scheduler integration and configure TickerQ with the provided
    /// <paramref name="configureTickerQ"/> delegate.
    /// <para>
    /// This method:
    /// <list type="bullet">
    ///   <item><description>Registers the InterBus delivery function with <c>TickerFunctionProvider</c>.</description></item>
    ///   <item><description>Calls <c>AddTickerQ</c> on the service collection with the provided options.</description></item>
    ///   <item><description>Registers <see cref="TickerQMessageScheduler"/> as the <see cref="IMessageScheduler"/>.</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// TickerQ defaults to an in-memory persistence provider when no operational store is
    /// configured. For durable scheduling across application restarts, chain
    /// <c>.AddOperationalStore(...)</c> on the <paramref name="configureTickerQ"/> builder.
    /// </para>
    /// </summary>
    /// <param name="builder">The InterBus builder.</param>
    /// <param name="configureTickerQ">
    /// Optional delegate to further configure TickerQ (e.g. call <c>AddOperationalStore</c> to
    /// enable EF Core persistence, or <c>ConfigureScheduler</c> to tune polling intervals).
    /// </param>
    public static IInterBusBuilder AddTickerQScheduler(
        this IInterBusBuilder builder,
        Action<TickerOptionsBuilder<TimeTickerEntity, CronTickerEntity>>? configureTickerQ = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        // Register the interbus-deliver function with TickerQ before the host starts.
        // TickerFunctionProvider is a static registry; calling RegisterFunctions multiple times
        // is safe — later registrations with the same key overwrite earlier ones.
        TickerFunctionProvider.RegisterFunctions(
            new Dictionary<string, (string, TickerTaskPriority, TickerFunctionDelegate, int)>
            {
                [TickerQDeliverFunction.FunctionName] = (
                    TickerQDeliverFunction.FunctionName,
                    TickerTaskPriority.Normal,
                    TickerQDeliverFunction.InvokeAsync,
                    0)
            });

        builder.Services.AddTickerQ(options =>
        {
            configureTickerQ?.Invoke(options);
        });

        builder.Services.AddSingleton<IMessageScheduler, TickerQMessageScheduler>();

        return builder;
    }
}
