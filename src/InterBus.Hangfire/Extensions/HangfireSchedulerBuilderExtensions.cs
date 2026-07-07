using InterBus.Scheduling;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.Hangfire.Extensions;

/// <summary>Extension methods for configuring the Hangfire scheduler on a <see cref="SchedulerBuilder"/>.</summary>
public static class HangfireSchedulerBuilderExtensions
{
    /// <param name="builder">The InterBus scheduler builder.</param>
    extension(SchedulerBuilder builder)
    {
        /// <summary>
        /// Register the Hangfire scheduler integration for InterBus.
        /// <para>
        /// This method registers <see cref="HangfireDeliverFunction"/> and
        /// <see cref="HangfireMessageScheduler"/> as the <see cref="IMessageSchedulingProvider"/>.
        /// </para>
        /// <para>
        /// <strong>Prerequisites:</strong>
        /// <list type="bullet">
        ///   <item><description>
        ///     Register Hangfire in the host's service collection (e.g. <c>services.AddHangfire(...)</c>)
        ///     so <c>IBackgroundJobClient</c> is available in DI.
        ///   </description></item>
        ///   <item><description>
        ///     Call <c>bus.AddScheduling()</c> before this method to register the <see cref="IScheduler"/> infrastructure.
        ///   </description></item>
        /// </list>
        /// </para>
        /// </summary>
        public SchedulerBuilder AddHangfireScheduler()
        {
            builder.Services.AddTransient<HangfireDeliverFunction>();
            builder.Services.AddSingleton<IMessageSchedulingProvider, HangfireMessageScheduler>();

            return builder;
        }
    }
}
