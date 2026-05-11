using InterBus.Generated;
using InterBus.Transporting;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.Scheduling;

public class SchedulerBuilder(InterBusBuilder bus, IServiceCollection services)
{
    public InterBusBuilder Bus => bus;
    public IServiceCollection Services => services;
    
    /// <summary>
        /// Register a publish-based <see cref="IScheduler"/> that schedules messages by
        /// publishing a concrete command message to the bus. The command is consumed by a
        /// <see cref="SchedulingConsumer"/> (registered via <see cref="AddSchedulingConsumer"/>),
        /// which enlists the registered <see cref="IMessageSchedulingProvider"/> to perform the
        /// actual deferred delivery.
        /// <para>
        /// Use <see cref="AddSchedulingConsumer"/> in the same application or in a dedicated
        /// consumer service to process the scheduling commands.
        /// </para>
        /// </summary>
        public SchedulerBuilder ViaSend()
        {
            services.AddScoped<IMessageScheduler, SendMessageScheduler>();
            return this;
        }

        /// <summary>
        /// Register the <see cref="SchedulingConsumer"/> that processes scheduling command messages sent by
        /// <see cref="SchedulerBuilder.ViaSend"/> and enlist the registered
        /// <see cref="IMessageSchedulingProvider"/> to perform the actual deferred delivery or
        /// cancellation.
        /// <para>
        /// Requires an <see cref="IMessageSchedulingProvider"/> to be registered in DI
        /// (e.g. via <c>AddTickerQScheduler()</c>).
        /// </para>
        /// </summary>
        public SchedulerBuilder AddSchedulingConsumer()
        {
            bus.AddConsumer<SchedulingConsumer>();
            return this;
        }

        /// <summary>
        /// Register the transport-delay scheduler. Scheduled messages are delivered
        /// immediately via the configured transport with a <see cref="TransportSendContext.Delay"/>
        /// equal to the requested deferral duration. Transports that support deferred visibility
        /// (e.g. PostgreSQL <c>visible_at</c>) will honour the delay; transports that do not will
        /// deliver the message immediately.
        /// <para>
        /// This provider requires no additional dependencies beyond the configured transport and
        /// is suitable for environments where a dedicated scheduler (e.g. TickerQ) is not available.
        /// </para>
        /// </summary>
        public SchedulerBuilder OnTransport()
        {
            services.AddScoped<IMessageScheduler, TransportDelayScheduler>();
            return this;
        }

        /// <summary>
        /// Configures the scheduler to use a direct scheduling mechanism by registering
        /// the <see cref="DirectMessageScheduler"/> implementation for deferred message delivery.
        /// This approach directly forwards scheduled messages to a registered
        /// <see cref="IMessageSchedulingProvider"/> without relying on publish-based scheduling or
        /// transport-level delays.
        /// </summary>
        public SchedulerBuilder Directly()
        {
            services.AddScoped<IMessageScheduler, DirectMessageScheduler>();
            return this;
        }
}
