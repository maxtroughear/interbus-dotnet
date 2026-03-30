namespace InterBus.TickerQ;

/// <summary>
/// Serializable payload stored in TickerQ's <c>TimeTickerEntity.Request</c> field for each
/// scheduled InterBus message. Carries all the information required to deliver the message to
/// the transport when the TickerQ job fires.
/// </summary>
public sealed class ScheduledMessageRequest
{
    /// <summary>
    /// The destination topic name (when <see cref="IsTopic"/> is <c>true</c>) or queue name
    /// (when <see cref="IsTopic"/> is <c>false</c>).
    /// </summary>
    public string Destination { get; init; } = string.Empty;

    /// <summary>
    /// <c>true</c> if the message should be published to a topic (fan-out);
    /// <c>false</c> if it should be sent directly to a queue (point-to-point).
    /// </summary>
    public bool IsTopic { get; init; }

    /// <summary>The serialized message payload bytes.</summary>
    public byte[] Payload { get; init; } = [];

    /// <summary>The message headers to forward to the transport on delivery.</summary>
    public Dictionary<string, string> Headers { get; init; } = [];

    /// <summary>Parameterless constructor required for JSON deserialization.</summary>
    public ScheduledMessageRequest() { }

    internal ScheduledMessageRequest(string destination, bool isTopic, byte[] payload, MessageHeaders headers)
    {
        Destination = destination;
        IsTopic = isTopic;
        Payload = payload;
        Headers = new Dictionary<string, string>(headers);
    }
}
