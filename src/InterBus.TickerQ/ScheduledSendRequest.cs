namespace InterBus.TickerQ;

/// <summary>
/// Serializable payload stored in TickerQ's <c>TimeTickerEntity.Request</c> field for each
/// scheduled send (point-to-point) InterBus message.
/// </summary>
public sealed class ScheduledSendRequest
{
    /// <summary>The queue name to send to (point-to-point, bypassing topic fan-out).</summary>
    public string QueueName { get; init; } = string.Empty;

    /// <summary>The serialized message payload bytes.</summary>
    public byte[] Payload { get; init; } = [];

    /// <summary>The message headers to forward to the transport on delivery.</summary>
    public Dictionary<string, string> Headers { get; init; } = [];

    /// <summary>Parameterless constructor required for JSON deserialization.</summary>
    public ScheduledSendRequest() { }

    internal ScheduledSendRequest(string queueName, byte[] payload, MessageHeaders headers)
    {
        QueueName = queueName;
        Payload = payload;
        Headers = new Dictionary<string, string>(headers);
    }
}
