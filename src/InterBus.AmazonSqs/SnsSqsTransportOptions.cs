using InterBus.Retry;

namespace InterBus.AmazonSqs;

/// <summary>
/// Configuration options for the Amazon SNS/SQS transport.
/// <para>
/// AWS credentials and region are resolved by the AWS SDK using the standard provider chain
/// (environment variables, shared credentials file, IAM instance profile, etc.). Use
/// <see cref="Microsoft.Extensions.DependencyInjection.ServiceCollectionExtensions.AddDefaultAWSOptions"/>
/// on the host's <c>IServiceCollection</c> before calling
/// <c>AddAmazonSqsTransport</c> if you need to supply explicit credentials or a specific region.
/// </para>
/// </summary>
public sealed class SnsSqsTransportOptions
{
    /// <summary>
    /// Optional service URL override. Use this to point at a local emulator such as
    /// LocalStack (<c>http://localhost:4566</c>). When <c>null</c> the default AWS
    /// regional endpoint is used.
    /// <para>
    /// Setting this property overrides the standard SDK endpoint resolution for both the
    /// SNS and SQS clients created by this transport.
    /// </para>
    /// </summary>
    public string? ServiceUrl { get; set; }

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
