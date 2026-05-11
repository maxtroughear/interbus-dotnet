using InterBus.Scheduling;
using InterBus.TickerQ.Internal;
using Microsoft.Extensions.DependencyInjection;
using TickerQ.Utilities;
using TickerQ.Utilities.Base;
using TickerQ.Utilities.Enums;

namespace InterBus.TickerQ.Extensions;

/// <summary>Extension methods for configuring the TickerQ scheduler on an <see cref="InterBusBuilder"/>.</summary>
public static class TickerQSchedulerBuilderExtensions
{
    /// <param name="builder">The InterBus builder.</param>
    extension(SchedulerBuilder builder)
    {
        /// <summary>
        /// Register the TickerQ scheduler integration for InterBus.
        /// <para>
        /// This method registers only the TickerQ-specific scheduling infrastructure:
        /// <list type="bullet">
        ///   <item><description>
        ///     Registers the InterBus delivery function (<c>interbus-deliver</c>) and its request
        ///     type with <c>TickerFunctionProvider</c>.
        ///   </description></item>
        ///   <item><description>
        ///     Registers <see cref="TickerQDeliverFunction"/> in the service collection for DI
        ///     constructor injection.
        ///   </description></item>
        ///   <item><description>
        ///     Registers <see cref="TickerQMessageScheduler"/> as the
        ///     <see cref="IMessageSchedulingProvider"/>.
        ///   </description></item>
        /// </list>
        /// </para>
        /// <para>
        /// <strong>Note:</strong> Call <c>bus.AddScheduling()</c> from the core InterBus library
        /// before calling this method to register the <see cref="IScheduler"/> infrastructure.
        /// TickerQ itself (<c>AddTickerQ</c>) must also be registered separately in application startup code.
        /// </para>
        /// </summary>
        public SchedulerBuilder AddTickerQScheduler()
        {
            ArgumentNullException.ThrowIfNull(builder);

            // Register the interbus-deliver function with TickerQ before the host starts.
            // TickerFunctionProvider is a static registry; calling RegisterFunctions multiple times
            // is safe - later registrations with the same key overwrite earlier ones.
            TickerFunctionProvider.RegisterFunctions(
                new Dictionary<string, (string, TickerTaskPriority, TickerFunctionDelegate, int)>
                {
                    [TickerQDeliverFunction.FunctionName] = (
                        TickerQDeliverFunction.FunctionName,
                        TickerTaskPriority.Normal,
                        async (ct, sp, ctx) =>
                        {
                            var typedCtx = (TickerFunctionContext<ScheduledDeliveryRequest>)ctx;
                            var func = sp.GetRequiredService<TickerQDeliverFunction>();
                            await func.InvokeAsync(typedCtx, ct);
                        },
                        0)
                });

            // Register request type so TickerQ deserializes the request bytes and populates
            // the typed TickerFunctionContext<T>.Request before calling the delegate.
            TickerFunctionProvider.RegisterRequestType(
                new Dictionary<string, (string, Type)>
                {
                    [TickerQDeliverFunction.FunctionName] = (
                        TickerQDeliverFunction.FunctionName, typeof(ScheduledDeliveryRequest))
                });

            // Register the function handler class for DI constructor injection.
            builder.Services.AddTransient<TickerQDeliverFunction>();

            // Register the TickerQ scheduling provider.
            builder.Services.AddSingleton<IMessageSchedulingProvider, TickerQMessageScheduler>();

            return builder;
        }
    }
}
