using Amazon.Scheduler;
using Amazon.Scheduler.Model;

namespace InterBus.AmazonSqs;

/// <summary>
/// Internal abstraction over <see cref="IAmazonScheduler"/> for creating and deleting
/// EventBridge Scheduler schedules. Simplifies unit testing.
/// </summary>
internal interface IEventBridgeScheduleStore
{
    /// <summary>Creates an EventBridge Scheduler schedule.</summary>
    Task CreateScheduleAsync(CreateScheduleRequest request, CancellationToken cancellationToken);

    /// <summary>Deletes an EventBridge Scheduler schedule by name.</summary>
    Task DeleteScheduleAsync(DeleteScheduleRequest request, CancellationToken cancellationToken);
}

/// <summary>
/// Default <see cref="IEventBridgeScheduleStore"/> implementation backed by
/// <see cref="IAmazonScheduler"/>.
/// </summary>
internal sealed class AmazonEventBridgeScheduleStore(IAmazonScheduler client) : IEventBridgeScheduleStore
{
    public Task CreateScheduleAsync(CreateScheduleRequest request, CancellationToken cancellationToken) =>
        client.CreateScheduleAsync(request, cancellationToken);

    public Task DeleteScheduleAsync(DeleteScheduleRequest request, CancellationToken cancellationToken) =>
        client.DeleteScheduleAsync(request, cancellationToken);
}

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
public sealed class EventBridgeSchedulingProvider : IMessageSchedulingProvider
{
    private readonly IEventBridgeScheduleStore _scheduleStore;
    private readonly ITopologyBuilder _topologyBuilder;
    private readonly EventBridgeSchedulerOptions _options;

    /// <summary>
    /// Initializes a new instance of <see cref="EventBridgeSchedulingProvider"/> using
    /// the real <see cref="IAmazonScheduler"/> client.
    /// </summary>
    public EventBridgeSchedulingProvider(
        IAmazonScheduler schedulerClient,
        ITopologyBuilder topologyBuilder,
        EventBridgeSchedulerOptions options)
        : this(new AmazonEventBridgeScheduleStore(schedulerClient), topologyBuilder, options)
    {
    }

    /// <summary>
    /// Internal constructor for unit testing - accepts abstracted adapter interfaces.
    /// </summary>
    internal EventBridgeSchedulingProvider(
        IEventBridgeScheduleStore scheduleStore,
        ITopologyBuilder topologyBuilder,
        EventBridgeSchedulerOptions options)
    {
        _scheduleStore = scheduleStore;
        _topologyBuilder = topologyBuilder;
        _options = options;
    }

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

        await _scheduleStore.CreateScheduleAsync(
            new CreateScheduleRequest
            {
                Name = scheduleId.ToString(),
                GroupName = _options.ScheduleGroupName,
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
                    RoleArn = _options.TargetRoleArn,
                    Input = input,
                },
                // Automatically clean up the schedule once it has fired.
                ActionAfterCompletion = ActionAfterCompletion.DELETE,
            },
            cancellationToken);
    }

    /// <inheritdoc/>
    public Task CancelAsync(Guid scheduleId, CancellationToken cancellationToken) =>
        _scheduleStore.DeleteScheduleAsync(
            new DeleteScheduleRequest
            {
                Name = scheduleId.ToString(),
                GroupName = _options.ScheduleGroupName,
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
            // EnsureTopicAsync returns the SNS topic ARN.
            return await _topologyBuilder.EnsureTopicAsync(rawTopicName, cancellationToken);
        }

        if (destination.Scheme == SqsTransport.QueueScheme)
        {
            var rawQueueName = RoutingConventions.GetDestinationName(destination);
            // EnsureQueueArnAsync returns the SQS queue ARN (cached in the topology builder).
            return await _topologyBuilder.EnsureQueueArnAsync(rawQueueName, cancellationToken);
        }

        throw new ArgumentException(
            $"Unsupported destination URI scheme '{destination.Scheme}' for EventBridge Scheduler. " +
            $"Expected '{SnsTransport.TopicScheme}' or '{SqsTransport.QueueScheme}'.",
            nameof(destination));
    }
}
