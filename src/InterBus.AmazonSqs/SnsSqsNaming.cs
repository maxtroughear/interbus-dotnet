using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Web;

namespace InterBus.AmazonSqs;

/// <summary>
/// Singleton service that provides SNS topic name and SQS queue name sanitisation and
/// caches resolved topic ARNs and queue URLs so that the AWS describe/create calls are
/// not repeated on every send.
/// </summary>
public sealed class SnsSqsNaming(SnsSqsTransportOptions options)
{
    private static readonly Regex InvalidNameChars =
        new(@"[^A-Za-z0-9\-_]", RegexOptions.Compiled, TimeSpan.FromSeconds(1));

    private static readonly Regex MultipleHyphens =
        new(@"-{2,}", RegexOptions.Compiled, TimeSpan.FromSeconds(1));
    
    private const int MaxSqsQueueNameLength = 80;
    private const int MaxSnsTopicNameLength = 256;
    private const string FifoSuffix = ".fifo";

    /// <summary>
    /// Converts a raw InterBus topic name (e.g. a fully-qualified .NET type name) into a
    /// valid SNS topic name, replacing dots and other invalid characters with hyphens and
    /// trimming to the 256-character limit. Appends <c>.fifo</c> for FIFO message types.
    /// </summary>
    public string BuildSnsTopicName(Uri topic)
    {
        var topicName = RoutingConventions.GetDestinationName(topic);
        var isFifo = HttpUtility.ParseQueryString(topic.Query)["fifo"] == "true";
        var suffix = isFifo ? FifoSuffix : string.Empty;

        var maxSanitizedLength = MaxSnsTopicNameLength
                                 - options.TopicPrefix.Length
                                 - options.TopicSuffix.Length
                                 - suffix.Length;
        var sanitized = SanitizeName(topicName, maxSanitizedLength);
        return options.TopicPrefix + sanitized + options.TopicSuffix + suffix;
    }

    /// <summary>
    /// Converts a raw InterBus queue name (e.g. a fully-qualified .NET consumer type name)
    /// into a valid SQS queue name, replacing invalid characters with hyphens and, if the
    /// result would exceed 80 characters, appending an 8-character hex hash to ensure
    /// uniqueness. Appends <c>.fifo</c> for FIFO consumers.
    /// </summary>
    public string BuildSqsQueueName(Uri queue)
    {
        var queueName = RoutingConventions.GetDestinationName(queue);
        var isFifo = HttpUtility.ParseQueryString(queue.Query)["fifo"] == "true";
        var suffix = isFifo ? FifoSuffix : string.Empty;

        var maxSanitizedLength = MaxSqsQueueNameLength
                                 - options.QueuePrefix.Length
                                 - options.QueueSuffix.Length
                                 - suffix.Length;
        var sanitized = SanitizeName(queueName, maxSanitizedLength);
        return options.QueuePrefix + sanitized + options.QueueSuffix + suffix;
    }

    public string BuildDeadLetterQueueName(Uri queue)
    {
        var deadLetterQueue = new UriBuilder(queue)
        {
            Path = queue.AbsolutePath + options.DeadLetterQueueSuffix
        };
        return BuildSqsQueueName(deadLetterQueue.Uri);
    }

    private static string SanitizeName(string rawName, int maxLength)
    {
        rawName = InvalidNameChars.Replace(rawName, "-");
        rawName = MultipleHyphens.Replace(rawName, "-");
        rawName = rawName.Trim('-');
        
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
}
