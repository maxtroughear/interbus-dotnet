namespace InterBus;

/// <summary>
/// Describes a registered consumer and the message type it handles.
/// Includes a pre-compiled dispatch delegate to invoke the consumer without runtime reflection during message processing.
/// </summary>
public sealed class ConsumerDescriptor
{
    /// <summary>The concrete consumer type.</summary>
    public Type ConsumerType { get; }

    /// <summary>The message type handled by this consumer.</summary>
    public Type MessageType { get; }

    /// <summary>The topic name this consumer subscribes to (derived from <see cref="MessageType"/> by convention).</summary>
    public string TopicName { get; }

    /// <summary>The queue name for this consumer (derived from consumer and message type by convention).</summary>
    public string QueueName { get; }

    /// <summary>
    /// Pre-compiled dispatch delegate. Resolves the consumer from the provided <see cref="IServiceProvider"/>,
    /// deserialises the payload, and invokes <c>Consume</c>.
    /// </summary>
    public Func<IServiceProvider, byte[], CancellationToken, Task> Dispatch { get; }

    internal ConsumerDescriptor(
        Type consumerType,
        Type messageType,
        string topicName,
        string queueName,
        Func<IServiceProvider, byte[], CancellationToken, Task> dispatch)
    {
        ConsumerType = consumerType;
        MessageType = messageType;
        TopicName = topicName;
        QueueName = queueName;
        Dispatch = dispatch;
    }
}
