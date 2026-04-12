using InterBus.Retry;

namespace InterBus.AmazonSqs;

/// <summary>
/// Configuration options for the Amazon SNS/SQS transport.
/// </summary>
public sealed class SnsSqsTransportOptions
{
    /// <summary>
    /// The AWS region endpoint name (e.g. <c>us-east-1</c>).
    /// Defaults to <c>us-east-1</c>.
    /// </summary>
    public string Region { get; set; } = "us-east-1";

    /// <summary>
    /// Optional service URL override. Use this to point at a local emulator such as
    /// LocalStack (<c>http://localhost:4566</c>). When <c>null</c> the default AWS
    /// regional endpoint is used.
    /// </summary>
    public string? ServiceUrl { get; set; }

    /// <summary>
    /// Optional AWS access key. When <c>null</c> or empty the standard AWS credential
    /// provider chain (environment variables, instance profile, etc.) is used.
    /// </summary>
    public string? AccessKey { get; set; }

    /// <summary>
    /// Optional AWS secret key. Ignored unless <see cref="AccessKey"/> is also set.
    /// </summary>
    public string? SecretKey { get; set; }

    /// <summary>
    /// Prefix applied to SNS topic names. Defaults to <c>interbus-topic-</c>.
    /// </summary>
    public string TopicPrefix { get; set; } = "interbus-topic-";

    /// <summary>
    /// Prefix applied to SQS queue names. Defaults to <c>interbus-queue-</c>.
    /// </summary>
    public string QueuePrefix { get; set; } = "interbus-queue-";

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
