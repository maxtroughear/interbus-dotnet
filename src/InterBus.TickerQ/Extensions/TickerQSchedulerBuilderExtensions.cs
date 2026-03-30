using InterBus.Internal;
using InterBus.TickerQ.Internal;
using Microsoft.Extensions.DependencyInjection;
using TickerQ.Utilities;
using TickerQ.Utilities.Base;
using TickerQ.Utilities.Entities;
using TickerQ.Utilities.Enums;

namespace InterBus.TickerQ.Extensions;

/// <summary>Extension methods for configuring the TickerQ scheduler on an <see cref="IInterBusBuilder"/>.</summary>
public static class TickerQSchedulerBuilderExtensions
{
    /// <summary>
    /// Register the TickerQ scheduler integration for InterBus.
    /// <para>
    /// This method:
    /// <list type="bullet">
    ///   <item><description>
    ///     Registers the InterBus delivery functions (<c>interbus-publish</c> and
    ///     <c>interbus-send</c>) and their request types with <c>TickerFunctionProvider</c>.
    ///   </description></item>
    ///   <item><description>
    ///     Registers <see cref="TickerQPublishFunction"/> and <see cref="TickerQSendFunction"/>
    ///     in the service collection for DI constructor injection.
    ///   </description></item>
    ///   <item><description>
    ///     Registers <see cref="TickerQMessageScheduler"/> as the
    ///     <see cref="IMessageSchedulingProvider"/>.
    ///   </description></item>
    ///   <item><description>
    ///     Registers <see cref="SchedulingMessagePublisher"/> as the
    ///     <see cref="IMessageScheduler"/>.
    ///   </description></item>
    /// </list>
    /// </para>
    /// <para>
    /// <strong>Note:</strong> TickerQ itself (<c>AddTickerQ</c>) must be registered separately
    /// in application startup code before building the host.
    /// </para>
    /// </summary>
    /// <param name="builder">The InterBus builder.</param>
    public static IInterBusBuilder AddTickerQScheduler(this IInterBusBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        // Register the interbus-publish and interbus-send functions with TickerQ before the host starts.
        // TickerFunctionProvider is a static registry; calling RegisterFunctions multiple times
        // is safe — later registrations with the same key overwrite earlier ones.
        TickerFunctionProvider.RegisterFunctions(
            new Dictionary<string, (string, TickerTaskPriority, TickerFunctionDelegate, int)>
            {
                [TickerQPublishFunction.FunctionName] = (
                    TickerQPublishFunction.FunctionName,
                    TickerTaskPriority.Normal,
                    async (ct, sp, ctx) =>
                    {
                        var typedCtx = (TickerFunctionContext<ScheduledPublishRequest>)ctx;
                        var func = sp.GetRequiredService<TickerQPublishFunction>();
                        await func.InvokeAsync(typedCtx, ct);
                    },
                    0),

                [TickerQSendFunction.FunctionName] = (
                    TickerQSendFunction.FunctionName,
                    TickerTaskPriority.Normal,
                    async (ct, sp, ctx) =>
                    {
                        var typedCtx = (TickerFunctionContext<ScheduledSendRequest>)ctx;
                        var func = sp.GetRequiredService<TickerQSendFunction>();
                        await func.InvokeAsync(typedCtx, ct);
                    },
                    0)
            });

        // Register request types so TickerQ deserializes the request bytes and populates
        // the typed TickerFunctionContext<T>.Request before calling the delegate.
        TickerFunctionProvider.RegisterRequestType(
            new Dictionary<string, (string, Type)>
            {
                [TickerQPublishFunction.FunctionName] = (
                    TickerQPublishFunction.FunctionName, typeof(ScheduledPublishRequest)),
                [TickerQSendFunction.FunctionName] = (
                    TickerQSendFunction.FunctionName, typeof(ScheduledSendRequest))
            });

        // Register the function handler classes for DI constructor injection.
        builder.Services.AddTransient<TickerQPublishFunction>();
        builder.Services.AddTransient<TickerQSendFunction>();

        // Register the scheduling provider and the scheduling publisher.
        builder.Services.AddSingleton<IMessageSchedulingProvider, TickerQMessageScheduler>();
        builder.Services.AddScoped<IMessageScheduler, SchedulingMessagePublisher>();

        return builder;
    }
}

