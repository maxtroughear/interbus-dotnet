using InterBus.Retry;
using Microsoft.Extensions.Hosting;

namespace InterBus.AmazonSqs;

/// <summary>
/// Configuration options for the Amazon SNS/SQS transport.
/// <para>
/// The transport requires <see cref="Amazon.SQS.IAmazonSQS"/> and
/// <see cref="Amazon.SimpleNotificationService.IAmazonSimpleNotificationService"/> to be
/// registered in the host's DI container <em>before</em> calling
/// <c>AddAmazonSqsTransport</c>. Use the AWS SDK DI extensions
/// (<c>AddAWSService&lt;T&gt;</c>) or a compatible library such as
/// <c>LocalStack.Client.Extensions</c> to register clients.
/// </para>
/// </summary>
public sealed class SnsSqsTransportOptions
{
    /// <summary>
    /// Prefix applied to SNS topic names. Defaults to <c>interbus-</c>.
    /// </summary>
    public string TopicPrefix { get; set; } = "interbus-";

    /// <summary>
    /// Prefix applied to SQS queue names. Defaults to <c>interbus-</c>.
    /// </summary>
    public string QueuePrefix { get; set; } = "interbus-";

    /// <summary>
    /// SQS message visibility timeout. When a message is received its visibility is set to
    /// this duration. The message pump automatically extends the visibility at
    /// <see cref="LeaseRenewalInterval"/> intervals while the message is still being
    /// processed, preventing redelivery for long-running consumers.
    /// <para>
    /// Defaults to <c>30</c> seconds. SQS accepts values from <c>0</c> to
    /// <c>43200</c> seconds (12 hours); fractional seconds are truncated.
    /// </para>
    /// </summary>
    public TimeSpan VisibilityTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// How often the message pump extends the visibility timeout of in-flight messages to
    /// prevent inadvertent redelivery during processing. When <c>null</c> (the default) the
    /// interval is inferred as half of <see cref="VisibilityTimeout"/>, matching the
    /// behaviour of the PostgreSQL lease-based pump.
    /// </summary>
    public TimeSpan? LeaseRenewalInterval { get; set; } = null;

    /// <summary>
    /// Number of additional messages to fetch beyond the number of currently available
    /// processing slots. A value greater than zero allows the pump to prefetch messages
    /// before all in-flight messages have finished, trading higher SQS visibility usage
    /// for lower latency at high throughput.
    /// <para>
    /// Defaults to <c>0</c> (no prefetch; fetch exactly as many messages as there are
    /// free processing slots).
    /// </para>
    /// </summary>
    public int PrefetchCount { get; set; } = 0;

    /// <summary>
    /// Configures retry behaviour for topology-setup failures in hosted services such
    /// as the message pump.
    /// <para>
    /// Defaults to effectively unbounded retries with exponential backoff from 1 second
    /// up to 30 seconds.
    /// </para>
    /// </summary>
    public RetryOptions ConnectionRetry { get; set; } = new()
    {
        RetryCount = int.MaxValue,
        DelayProvider = RetryDelays.Exponential(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30)),
    };

    /// <summary>
    /// Controls whether the topology (SQS queues, SNS topics, subscriptions, and resource
    /// policies) is created at startup or only verified against pre-existing resources.
    /// <para>
    /// Use <see cref="TopologyMode.VerifyOnly"/> when the runtime role does not have
    /// create/subscribe permissions and the infrastructure is managed out-of-band (Terraform,
    /// CloudFormation, AWS CDK, etc.) or by a separate initialization step.
    /// </para>
    /// <para>
    /// Defaults to <see cref="TopologyMode.CreateOrVerify"/>.
    /// </para>
    /// </summary>
    public TopologyMode TopologyMode { get; set; } = TopologyMode.CreateOrVerify;

    /// <summary>
    /// When <c>true</c> (the default), SQS queues created by InterBus are encrypted at rest
    /// using SSE-SQS (SQS-managed encryption keys). Set to <c>false</c> to opt out of
    /// server-side encryption, or when encryption is managed out-of-band (Terraform,
    /// CloudFormation, AWS CDK, etc.).
    /// <para>
    /// Has no effect when <see cref="TopologyMode"/> is <see cref="TopologyMode.VerifyOnly"/>.
    /// </para>
    /// </summary>
    public bool EnableSqsEncryption { get; set; } = true;

    /// <summary>
    /// When <c>true</c> (the default), SNS topics created by InterBus are encrypted at rest
    /// using the AWS managed key for SNS (<c>alias/aws/sns</c>). Set to <c>false</c> to opt
    /// out of server-side encryption, or when encryption is managed out-of-band (Terraform,
    /// CloudFormation, AWS CDK, etc.).
    /// <para>
    /// Has no effect when <see cref="TopologyMode"/> is <see cref="TopologyMode.VerifyOnly"/>.
    /// </para>
    /// </summary>
    public bool EnableSnsEncryption { get; set; } = true;

    /// <summary>
    /// Effective lease renewal interval: <see cref="LeaseRenewalInterval"/> when set,
    /// otherwise half of <see cref="VisibilityTimeout"/> (minimum 1 second).
    /// </summary>
    internal TimeSpan EffectiveLeaseRenewalInterval =>
        LeaseRenewalInterval
        ?? TimeSpan.FromSeconds(Math.Max(1.0, VisibilityTimeout.TotalSeconds / 2.0));
}
