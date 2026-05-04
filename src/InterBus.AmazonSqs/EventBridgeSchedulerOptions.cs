namespace InterBus.AmazonSqs;

/// <summary>
/// Configuration options for the Amazon EventBridge Scheduler integration.
/// </summary>
public sealed class EventBridgeSchedulerOptions
{
    /// <summary>
    /// The EventBridge Scheduler schedule group that all InterBus-created schedules belong to.
    /// Defaults to <c>"default"</c> (the AWS-managed default group).
    /// </summary>
    public string ScheduleGroupName { get; set; } = "default";

    /// <summary>
    /// The ARN of the IAM role that EventBridge Scheduler assumes when invoking the target
    /// (SQS queue or SNS topic). The role must grant EventBridge Scheduler permission to
    /// send messages to the configured target.
    /// <para>
    /// This property must be set before the application starts.
    /// </para>
    /// </summary>
    public string TargetRoleArn { get; set; } = string.Empty;
}
