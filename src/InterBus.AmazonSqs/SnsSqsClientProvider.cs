using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using Amazon;
using Amazon.Runtime;
using Amazon.Runtime.Credentials;
using Amazon.SimpleNotificationService;
using Amazon.SQS;

namespace InterBus.AmazonSqs;

/// <summary>
/// Singleton service that creates and caches the
/// <see cref="IAmazonSimpleNotificationService"/> and <see cref="IAmazonSQS"/> clients and
/// tracks resolved topic ARNs and queue URLs so that the AWS describe/create calls are not
/// repeated on every send.
/// </summary>
internal sealed class SnsSqsClientProvider : IAsyncDisposable
{
    private static readonly Regex InvalidNameChars =
        new(@"[^A-Za-z0-9\-_]", RegexOptions.Compiled, TimeSpan.FromSeconds(1));

    private static readonly Regex MultipleHyphens =
        new(@"-{2,}", RegexOptions.Compiled, TimeSpan.FromSeconds(1));

    private const int MaxSqsQueueNameLength = 80;
    private const int MaxSnsTopicNameLength = 256;

    private readonly SnsSqsTransportOptions _options;

    internal IAmazonSimpleNotificationService SnsClient { get; }
    internal IAmazonSQS SqsClient { get; }

    /// <summary>
    /// Application-level cache: sanitised topic name → SNS Topic ARN.
    /// </summary>
    internal ConcurrentDictionary<string, string> TopicArns { get; } =
        new(StringComparer.Ordinal);

    /// <summary>
    /// Application-level cache: sanitised queue name → SQS Queue URL.
    /// </summary>
    internal ConcurrentDictionary<string, string> QueueUrls { get; } =
        new(StringComparer.Ordinal);

    public SnsSqsClientProvider(SnsSqsTransportOptions options)
    {
        _options = options;
        var credentials = BuildCredentials(options);
        var region = RegionEndpoint.GetBySystemName(options.Region);
        SnsClient = BuildSnsClient(credentials, region, options.ServiceUrl);
        SqsClient = BuildSqsClient(credentials, region, options.ServiceUrl);
    }

    /// <summary>
    /// Converts a raw InterBus topic name (e.g. a fully-qualified .NET type name) into a
    /// valid SNS topic name, replacing dots and other invalid characters with hyphens and
    /// trimming to the 256-character limit.
    /// </summary>
    internal string BuildSnsTopicName(string rawName)
    {
        var sanitized = SanitizeName(rawName, MaxSnsTopicNameLength - _options.TopicPrefix.Length);
        return _options.TopicPrefix + sanitized;
    }

    /// <summary>
    /// Converts a raw InterBus queue name (e.g. a fully-qualified .NET consumer type name)
    /// into a valid SQS queue name, replacing invalid characters with hyphens and, if the
    /// result would exceed 80 characters, appending an 8-character hex hash to ensure
    /// uniqueness.
    /// </summary>
    internal string BuildSqsQueueName(string rawName)
    {
        var maxSanitizedLength = MaxSqsQueueNameLength - _options.QueuePrefix.Length;
        var sanitized = SanitizeName(rawName, maxSanitizedLength);
        return _options.QueuePrefix + sanitized;
    }

    private static string SanitizeName(string rawName, int maxLength)
    {
        // Replace all invalid chars (everything except alphanumerics, hyphens, underscores)
        // with a hyphen.
        var sanitized = InvalidNameChars.Replace(rawName, "-");

        // Collapse consecutive hyphens.
        sanitized = MultipleHyphens.Replace(sanitized, "-");

        // Trim leading/trailing hyphens.
        sanitized = sanitized.Trim('-');

        if (sanitized.Length <= maxLength)
            return sanitized;

        // Truncate: keep the first (maxLength - 9) chars and append '-' + 8-hex-char hash.
        var hashSuffix = ComputeShortHash(rawName);
        var truncatedLength = Math.Max(0, maxLength - 9);
        return sanitized[..truncatedLength] + "-" + hashSuffix;
    }

    private static string ComputeShortHash(string input)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(input));
        return Convert.ToHexStringLower(bytes)[..8];
    }

    private static AWSCredentials BuildCredentials(SnsSqsTransportOptions options)
    {
        if (!string.IsNullOrWhiteSpace(options.AccessKey) &&
            !string.IsNullOrWhiteSpace(options.SecretKey))
        {
            return new BasicAWSCredentials(options.AccessKey, options.SecretKey);
        }

        // Use the standard AWS credential provider chain (environment variables,
        // shared credentials file, instance profile, etc.).
        return DefaultAWSCredentialsIdentityResolver.GetCredentials();
    }

    private static IAmazonSimpleNotificationService BuildSnsClient(
        AWSCredentials credentials,
        RegionEndpoint region,
        string? serviceUrl)
    {
        var config = new AmazonSimpleNotificationServiceConfig { RegionEndpoint = region };
        if (!string.IsNullOrWhiteSpace(serviceUrl))
            config.ServiceURL = serviceUrl;

        return new AmazonSimpleNotificationServiceClient(credentials, config);
    }

    private static IAmazonSQS BuildSqsClient(
        AWSCredentials credentials,
        RegionEndpoint region,
        string? serviceUrl)
    {
        var config = new AmazonSQSConfig { RegionEndpoint = region };
        if (!string.IsNullOrWhiteSpace(serviceUrl))
            config.ServiceURL = serviceUrl;

        return new AmazonSQSClient(credentials, config);
    }

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        SnsClient.Dispose();
        SqsClient.Dispose();
        return ValueTask.CompletedTask;
    }
}
