namespace InterBus.AmazonSqs;

/// <summary>
/// Controls how <see cref="SnsSqsTopologyBuilder"/> declares the SNS/SQS topology at startup.
/// </summary>
public enum TopologyMode
{
    /// <summary>
    /// Create SQS queues, SNS topics, subscriptions, and queue resource policies if they do
    /// not already exist (all AWS create/subscribe operations are idempotent). This is the
    /// default and suits development environments or applications that are authorised to
    /// manage their own infrastructure.
    /// </summary>
    CreateOrVerify,

    /// <summary>
    /// Assume the topology was provisioned externally (e.g. Terraform, CloudFormation, or a
    /// dedicated one-time initialization step). At startup the pump only reads the existing
    /// resources to populate its internal caches — no queues, topics, subscriptions, or
    /// resource policies are created. Use this when the runtime role does not have
    /// <c>sqs:CreateQueue</c>, <c>sns:CreateTopic</c>, or <c>sns:Subscribe</c> permissions.
    /// </summary>
    VerifyOnly,
}
