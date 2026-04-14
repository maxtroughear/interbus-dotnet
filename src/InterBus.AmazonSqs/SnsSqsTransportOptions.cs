using InterBus.Retry;

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
    /// Prefix applied to SNS topic names. Defaults to <c>interbus-topic-</c>.
    /// </summary>
    public string TopicPrefix { get; set; } = "interbus-topic-";

    /// <summary>
    /// Prefix applied to SQS queue names. Defaults to <c>interbus-queue-</c>.
    /// </summary>
    public string QueuePrefix { get; set; } = "interbus-queue-";

    /// <summary>
    /// SQS message visibility timeout in seconds. When a message is received its visibility
    /// is set to this duration. The message pump automatically extends the visibility by
    /// this amount at half-timeout intervals while the message is still being processed,
    /// preventing redelivery for long-running consumers.
    /// <para>
    /// Defaults to <c>30</c> seconds. Minimum is <c>1</c> second; maximum is <c>43200</c>
    /// seconds (12 hours).
    /// </para>
    /// </summary>
    public int VisibilityTimeoutSeconds { get; set; } = 30;

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
}
