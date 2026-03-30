namespace InterBus.TickerQ;

/// <summary>
/// Serializable payload stored in TickerQ's <c>TimeTickerEntity.Request</c> field for each
/// scheduled publish (topic fan-out) InterBus message.
/// </summary>
public sealed class ScheduledPublishRequest
{
    /// <summary>The topic name to publish to (fan-out to all subscriber queues).</summary>
    public string TopicName { get; init; } = string.Empty;

    /// <summary>The serialized message payload bytes.</summary>
    public byte[] Payload { get; init; } = [];

    /// <summary>The message headers to forward to the transport on delivery.</summary>
    public Dictionary<string, string> Headers { get; init; } = [];

    /// <summary>Parameterless constructor required for JSON deserialization.</summary>
    public ScheduledPublishRequest() { }

    internal ScheduledPublishRequest(string topicName, byte[] payload, MessageHeaders headers)
    {
        TopicName = topicName;
        Payload = payload;
        Headers = new Dictionary<string, string>(headers);
    }
}
