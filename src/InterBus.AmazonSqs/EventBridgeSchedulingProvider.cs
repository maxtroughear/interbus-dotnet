using Amazon.Scheduler;
using Amazon.Scheduler.Model;
using InterBus.Scheduling;

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
/// Register via <c>builder.AddEventBridgeScheduler()</c> on the <see cref="SchedulerBuilder"/>.
/// </para>
/// </summary>
/// <remarks>Because delivery is handled entirely by EventBridge Scheduler, some InterBus pipeline features are not
/// supported. And SNS message attributes (<c>interbus-message-type</c>, etc.) are not set when targeting SNS topics, so
/// SNS subscription filter policies based on those attributes will not match EventBridge-scheduled messages.
/// </remarks>
public sealed class EventBridgeSchedulingProvider(
    IAmazonScheduler schedulerClient,
    SnsSqsNaming naming,
    SnsSqsTopologyBuilder topology,
    ISnsSqsMessageEnvelope rawEnvelope,
    EventBridgeSchedulerOptions options)
    : IMessageSchedulingProvider
{
    /// <inheritdoc/>
    public async Task<string> ScheduleAsync(
        Uri destination,
        byte[] payload,
        MessageHeaders headers,
        DateTimeOffset scheduledAt,
        CancellationToken cancellationToken)
    {
        var scheduleId = Guid.CreateVersion7().ToString();
        var input = rawEnvelope.SerializeForSnsPublish(payload, headers);
        var targetArn = await ResolveTargetArnAsync(destination, cancellationToken);

        headers.TryGetValue(InterBusHeaders.MessageGroupId, out var messageGroupId);
        var isFifoSqsTarget =
            destination.Scheme == SqsTransport.QueueScheme &&
            targetArn.EndsWith(".fifo", StringComparison.Ordinal);

        await schedulerClient.CreateScheduleAsync(
            new CreateScheduleRequest
            {
                Name = scheduleId,
                GroupName = options.ScheduleGroupName,
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
                    RetryPolicy = new RetryPolicy
                    {
                        MaximumRetryAttempts = 10,
                        MaximumEventAgeInSeconds = 3600 // 1 hour
                    },
                    // EventBridge Scheduler validates MessageGroupId only for FIFO SQS targets.
                    SqsParameters = isFifoSqsTarget && messageGroupId is not null
                        ? new SqsParameters { MessageGroupId = messageGroupId }
                        : null,
                },
                ActionAfterCompletion = ActionAfterCompletion.DELETE,
            },
            cancellationToken);

        return scheduleId;
    }

    /// <inheritdoc/>
    public Task CancelAsync(string scheduleId, CancellationToken cancellationToken) =>
        schedulerClient.DeleteScheduleAsync(
            new DeleteScheduleRequest
            {
                Name = scheduleId,
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
            return await topology.EnsureTopicAsync(destination, cancellationToken);
        }

        if (destination.Scheme == SqsTransport.QueueScheme)
        {
            var queueName = naming.BuildSqsQueueName(destination);
            
            var queueUrl = await topology.ResolveQueueAsync(queueName, cancellationToken);
            
            // if (!topology.QueueUrls.TryGetValue(queueName, out var queueUrl))
            //     throw new InvalidOperationException(
            //         $"Queue '{queueName}' has not been declared. Ensure the consumer for this queue " +
            //         "is registered so that its queue is created during startup topology setup, or schedule " +
            //         "messages to an SNS topic instead.");

            if (!topology.QueueArns.TryGetValue(queueName, out var queueArn))
                throw new InvalidOperationException(
                    $"Queue ARN for '{queueName}' (resolved URL: '{queueUrl}') is not available " +
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
