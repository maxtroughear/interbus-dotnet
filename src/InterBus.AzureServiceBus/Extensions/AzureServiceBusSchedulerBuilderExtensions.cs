using InterBus.Scheduling;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.AzureServiceBus.Extensions;

/// <summary>Configures native Azure Service Bus scheduling.</summary>
public static class AzureServiceBusSchedulerBuilderExtensions
{
    /// <param name="builder">The InterBus scheduler builder.</param>
    extension(SchedulerBuilder builder)
    {
        /// <summary>
        /// Registers native scheduling using the connection configured by <c>AddAzureServiceBusTransport</c>.
        /// Target queues must already exist; topics are ensured by the transport topology builder.
        /// <para>
        /// Use <c>Directly()</c> to return an opaque schedule ID inline, or <c>ViaRequest()</c> with
        /// <c>AddSchedulingConsumer()</c> to return it through request/reply. <c>ViaPublish()</c> with
        /// the consumer returns null inline and publishes the ID in <c>ScheduleCreatedEvent</c>.
        /// </para>
        /// <para>
        /// IDs include the physical entity and are valid only in the configured Service Bus namespace.
        /// Cancellation cannot recall an activated message and may fail when racing activation.
        /// Broker failures propagate, including cancellation of missing schedules.
        /// <c>OnTransport()</c> retains transport delay semantics without cancellable IDs.
        /// </para>
        /// </summary>
        /// <returns>The scheduler builder for further configuration.</returns>
        public SchedulerBuilder AddAzureServiceBusScheduler()
        {
            builder.Services.AddSingleton<IMessageSchedulingProvider, AzureServiceBusSchedulingProvider>();
            return builder;
        }
    }
}
