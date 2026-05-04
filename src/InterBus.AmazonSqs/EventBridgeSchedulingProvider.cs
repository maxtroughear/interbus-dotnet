using Amazon.Scheduler;
using Amazon.Scheduler.Model;

namespace InterBus.AmazonSqs;

/// <summary>
/// <see cref="IMessageSchedulingProvider"/> implementation that uses Amazon EventBridge Scheduler
/// to deliver scheduled InterBus messages to SNS topics or SQS queues.
/// <para>
/// At the scheduled time, EventBridge Scheduler delivers the message payload directly to the
/// target (SNS topic or SQS queue) without requiring any compute to be running. Schedules
/// are automatically deleted by EventBridge after they fire.
/// </para>
/// <para>
/// Register via <c>builder.AddEventBridgeScheduler()</c> on the <see cref="ISchedulerBuilder"/>.
/// </para>
/// <para>
/// <strong>Caveats:</strong> Because delivery is handled entirely by EventBridge Scheduler,
/// some InterBus pipeline features are not supported:
/// <list type="bullet">
///   <item><description>
///     SNS message attributes (<c>interbus-message-type</c>, etc.) are not set when targeting
///     SNS topics, so SNS subscription filter policies based on those attributes will not match
///     EventBridge-scheduled messages.
///   </description></item>
///   <item><description>
///     Cancellation of already-fired schedules is not possible.
///   </description></item>
/// </list>
/// </para>
/// </summary>
public sealed class EventBridgeSchedulingProvider(
    IAmazonScheduler schedulerClient,
    SnsSqsTopology topology,
    ITopologyBuilder topologyBuilder,
    EventBridgeSchedulerOptions options)
    : IMessageSchedulingProvider
{
    /// <inheritdoc/>
    public async Task ScheduleAsync(
        Guid scheduleId,
        Uri destination,
        byte[] payload,
        MessageHeaders headers,
        DateTimeOffset scheduledAt,
        CancellationToken cancellationToken)
    {
        var input = SnsSqsMessageEnvelope.Serialize(payload, headers);
        var targetArn = await ResolveTargetArnAsync(destination, cancellationToken);

        await schedulerClient.CreateScheduleAsync(
            new CreateScheduleRequest
            {
                Name = scheduleId.ToString(),
                GroupName = options.ScheduleGroupName,
                // One-time schedule expressed as an absolute UTC timestamp.
                ScheduleExpression = $"at({scheduledAt.UtcDateTime:yyyy-MM-ddTHH:mm:ss})",
                ScheduleExpressionTimezone = "UTC",
                FlexibleTimeWindow = new FlexibleTimeWindow
                {
                    Mode = FlexibleTimeWindowMode.OFF,
                },
                Target = new Target
                {
                    Arn = targetArn,
                    RoleArn = options.TargetRoleArn,
                    Input = input,
                },
                // Automatically clean up the schedule once it has fired.
                ActionAfterCompletion = ActionAfterCompletion.DELETE,
            },
            cancellationToken);
    }

    /// <inheritdoc/>
    public Task CancelAsync(Guid scheduleId, CancellationToken cancellationToken) =>
        schedulerClient.DeleteScheduleAsync(
            new DeleteScheduleRequest
            {
                Name = scheduleId.ToString(),
                GroupName = options.ScheduleGroupName,
            },
            cancellationToken);

    /// <summary>
    /// Resolves the AWS ARN of the target resource (SQS queue ARN or SNS topic ARN) from the
    /// InterBus destination URI.
    /// </summary>
    private async Task<string> ResolveTargetArnAsync(Uri destination, CancellationToken cancellationToken)
    {
        if (destination.Scheme == SnsTransport.TopicScheme)
        {
            var rawTopicName = RoutingConventions.GetDestinationName(destination);
            return await topologyBuilder.EnsureTopicAsync(rawTopicName, cancellationToken);
        }

        if (destination.Scheme == SqsTransport.QueueScheme)
        {
            var rawQueueName = RoutingConventions.GetDestinationName(destination);
            var queueUrl = await topologyBuilder.EnsureQueueAsync(rawQueueName, cancellationToken);
            if (!topology.QueueArns.TryGetValue(queueUrl, out var queueArn))
                throw new InvalidOperationException(
                    $"Queue ARN for '{rawQueueName}' (resolved URL: '{queueUrl}') is not available " +
                    "in the SNS/SQS topology. Ensure the Amazon SQS transport has been configured " +
                    "and initialized before scheduling messages.");

            return queueArn;
        }

        throw new ArgumentException(
            $"Unsupported destination URI scheme '{destination.Scheme}' for EventBridge Scheduler. " +
            $"Expected '{SnsTransport.TopicScheme}' or '{SqsTransport.QueueScheme}'.",
            nameof(destination));
    }
}
