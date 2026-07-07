using Amazon.Scheduler;
using InterBus.Scheduling;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.AmazonSqs.Extensions;

/// <summary>
/// Extension methods for configuring the EventBridge Scheduler integration on an
/// <see cref="SchedulerBuilder"/>.
/// </summary>
public static class EventBridgeSchedulerBuilderExtensions
{
    /// <param name="builder">The InterBus scheduler builder.</param>
    extension(SchedulerBuilder builder)
    {
        /// <summary>
        /// Register the Amazon EventBridge Scheduler integration for InterBus.
        /// <para>
        /// This method registers <see cref="EventBridgeSchedulingProvider"/> as the
        /// <see cref="IMessageSchedulingProvider"/>. At the scheduled time EventBridge
        /// Scheduler delivers the message directly to the target SQS queue or SNS topic,
        /// with no compute required for delivery.
        /// </para>
        /// <para>
        /// <strong>Prerequisites:</strong>
        /// <list type="bullet">
        ///   <item><description>
        ///     Register <see cref="IAmazonScheduler"/> in the host's service collection
        ///     (e.g. via <c>services.AddAWSService&lt;IAmazonScheduler&gt;()</c>) before calling
        ///     this method.
        ///   </description></item>
        ///   <item><description>
        ///     Call <c>bus.AddAmazonSqsTransport()</c> before calling this method to ensure the
        ///     SNS/SQS topology services are registered.
        ///   </description></item>
        ///   <item><description>
        ///     Configure <see cref="EventBridgeSchedulerOptions.TargetRoleArn"/> with an IAM role
        ///     that grants EventBridge Scheduler permission to send to the target SQS queue or SNS topic.
        ///   </description></item>
        /// </list>
        /// </para>
        /// <para>
        /// <strong>Caveats:</strong> Because delivery is handled entirely by EventBridge Scheduler,
        /// SNS message attributes are not set when targeting SNS topics, so SNS subscription filter
        /// policies based on InterBus headers will not match EventBridge-scheduled messages.
        /// </para>
        /// </summary>
        /// <param name="configure">
        /// Optional delegate to configure <see cref="EventBridgeSchedulerOptions"/>.
        /// </param>
        public SchedulerBuilder AddEventBridgeScheduler(Action<EventBridgeSchedulerOptions>? configure = null)
        {
            var options = new EventBridgeSchedulerOptions { TargetRoleArn = string.Empty };
            configure?.Invoke(options);

            builder.Services.AddSingleton(options);
            builder.Services.AddSingleton<IMessageSchedulingProvider, EventBridgeSchedulingProvider>();

            return builder;
        }
    }
}
