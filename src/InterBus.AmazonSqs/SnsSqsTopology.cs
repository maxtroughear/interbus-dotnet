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
public sealed class SnsSqsTopology(SnsSqsTransportOptions options, InterBusOptions busOptions)
{
    private const int MaxSqsQueueNameLength = 80;
    private const int MaxSnsTopicNameLength = 256;
    private const string FifoSuffix = ".fifo";

    /// <summary>
    /// Raw SNS topic names (pre-prefix/suffix) for message types declared as FIFO.
    /// Computed once at construction from <see cref="InterBusOptions.FifoMessageTypes"/>.
    /// </summary>
    private readonly HashSet<string> _fifoTopicNames = BuildFifoTopicNames(busOptions);

    /// <summary>
    /// Raw SQS queue names (pre-prefix/suffix) for consumers declared as FIFO, including
    /// their DLQ names. Computed once at construction.
    /// </summary>
    private readonly HashSet<string> _fifoQueueNames = BuildFifoQueueNames(busOptions, options);

    /// <summary>
    /// Application-level cache: sanitised topic name → SNS Topic ARN
    /// </summary>
    internal ConcurrentDictionary<string, string> TopicArns { get; } =
        new(StringComparer.Ordinal);

    /// <summary>
    /// Application-level cache: sanitised queue name → SQS Queue URL
    /// </summary>
    internal ConcurrentDictionary<string, string> QueueUrls { get; } =
        new(StringComparer.Ordinal);

    /// <summary>
    /// Application-level cache: SQS Queue URL → SQS Queue ARN
    /// </summary>
    internal ConcurrentDictionary<string, string> QueueArns { get; } =
        new(StringComparer.Ordinal);

    /// <summary>
    /// Converts a raw InterBus topic name (e.g. a fully-qualified .NET type name) into a
    /// valid SNS topic name, replacing dots and other invalid characters with hyphens and
    /// trimming to the 256-character limit. Appends <c>.fifo</c> for FIFO message types.
    /// </summary>
    internal string BuildSnsTopicName(string rawName)
    {
        var isFifo = busOptions.
        
        var isFifo = _fifoTopicNames.Contains(rawName);
        var suffix = isFifo ? FifoSuffix : string.Empty;
        var maxSanitizedLength = MaxSnsTopicNameLength
                                 - options.TopicPrefix.Length
                                 - options.TopicSuffix.Length
                                 - suffix.Length;
        var sanitized = SanitizeName(rawName, maxSanitizedLength);
        return options.TopicPrefix + sanitized + options.TopicSuffix + suffix;
    }

    /// <summary>
    /// Converts a raw InterBus queue name (e.g. a fully-qualified .NET consumer type name)
    /// into a valid SQS queue name, replacing invalid characters with hyphens and, if the
    /// result would exceed 80 characters, appending an 8-character hex hash to ensure
    /// uniqueness. Appends <c>.fifo</c> for FIFO consumers.
    /// </summary>
    internal string BuildSqsQueueName(string rawName)
    {
        var isFifo = _fifoQueueNames.Contains(rawName);
        var suffix = isFifo ? FifoSuffix : string.Empty;
        var maxSanitizedLength = MaxSqsQueueNameLength
                                 - options.QueuePrefix.Length
                                 - options.QueueSuffix.Length
                                 - suffix.Length;
        var sanitized = SanitizeName(rawName, maxSanitizedLength);
        return options.QueuePrefix + sanitized + options.QueueSuffix + suffix;
    }

    internal string BuildDeadLetterQueueName(string rawQueueName) =>
        BuildSqsQueueName(rawQueueName + options.DeadLetterQueueSuffix);

    private static string SanitizeName(string rawName, int maxLength)
    {
        if (rawName.Length <= maxLength)
            return rawName;

        var hashSuffix = ComputeShortHash(rawName);
        var truncatedLength = Math.Max(0, maxLength - 9);
        return rawName[..truncatedLength] + "-" + hashSuffix;
    }

    private static string ComputeShortHash(string input)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(input));
        return Convert.ToHexStringLower(bytes)[..8];
    }

    private static HashSet<string> BuildFifoTopicNames(InterBusOptions opts)
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var type in opts.FifoMessageTypes)
        {
            var rawName = opts.TryGetTopicName(type, out var custom)
                ? custom
                : RoutingConventions.TopicName(type);
            names.Add(rawName);
        }

        return names;
    }

    private static HashSet<string> BuildFifoQueueNames(InterBusOptions opts, SnsSqsTransportOptions transportOptions)
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var consumer in opts.Consumers.Where(c => c.IsFifo))
        {
            names.Add(consumer.QueueName);
            names.Add(consumer.QueueName + transportOptions.DeadLetterQueueSuffix);
        }

        return names;
    }
}
