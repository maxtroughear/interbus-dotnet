using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace InterBus.AmazonSqs;

/// <summary>
/// Singleton service that provides SNS topic name and SQS queue name sanitisation and
/// caches resolved topic ARNs and queue URLs so that the AWS describe/create calls are
/// not repeated on every send.
/// </summary>
public sealed class SnsSqsClientProvider(SnsSqsTransportOptions options)
{
    private static readonly Regex InvalidNameChars =
        new(@"[^A-Za-z0-9\-_]", RegexOptions.Compiled, TimeSpan.FromSeconds(1));

    private static readonly Regex MultipleHyphens =
        new(@"-{2,}", RegexOptions.Compiled, TimeSpan.FromSeconds(1));

    private const int MaxSqsQueueNameLength = 80;
    private const int MaxSnsTopicNameLength = 256;

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

    /// <summary>
    /// Converts a raw InterBus topic name (e.g. a fully-qualified .NET type name) into a
    /// valid SNS topic name, replacing dots and other invalid characters with hyphens and
    /// trimming to the 256-character limit.
    /// </summary>
    internal string BuildSnsTopicName(string rawName)
    {
        var sanitized = SanitizeName(rawName, MaxSnsTopicNameLength - options.TopicPrefix.Length);
        return options.TopicPrefix + sanitized;
    }

    /// <summary>
    /// Converts a raw InterBus queue name (e.g. a fully-qualified .NET consumer type name)
    /// into a valid SQS queue name, replacing invalid characters with hyphens and, if the
    /// result would exceed 80 characters, appending an 8-character hex hash to ensure
    /// uniqueness.
    /// </summary>
    internal string BuildSqsQueueName(string rawName)
    {
        var maxSanitizedLength = MaxSqsQueueNameLength - options.QueuePrefix.Length;
        var sanitized = SanitizeName(rawName, maxSanitizedLength);
        return options.QueuePrefix + sanitized;
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
}
