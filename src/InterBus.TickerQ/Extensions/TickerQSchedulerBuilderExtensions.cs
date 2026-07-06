using InterBus.Scheduling;
using Microsoft.Extensions.DependencyInjection;
using TickerQ.DependencyInjection;

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
            builder.Services.MapTicker<TickerQDeliverFunction, ScheduledDeliveryRequest>();
            builder.Services.AddSingleton<IMessageSchedulingProvider, TickerQMessageScheduler>();

            return builder;
        }
    }
}
