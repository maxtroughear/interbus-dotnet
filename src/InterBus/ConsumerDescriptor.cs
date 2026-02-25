namespace InterBus;

/// <summary>
/// Describes a registered consumer and one of the message types it handles.
/// Each consumer class may have multiple <see cref="ConsumerDescriptor"/> instances - one per
/// message type - but they all share the same <see cref="QueueName"/> so the transport assigns
/// a single queue to that consumer class.
/// Includes a pre-compiled dispatch delegate to invoke the consumer without runtime reflection during message processing.
/// </summary>
public sealed class ConsumerDescriptor
{
    /// <summary>The concrete consumer type.</summary>
    public Type ConsumerType { get; }

    /// <summary>The message type handled by this descriptor.</summary>
    public Type MessageType { get; }

    /// <summary>The topic name this descriptor subscribes to (derived from <see cref="MessageType"/> by convention).</summary>
    public string TopicName { get; }

    /// <summary>
    /// The queue name for the consumer class. All descriptors for the same consumer share this
    /// name, resulting in exactly one queue per consumer regardless of how many message types it handles.
    /// </summary>
    public string QueueName { get; }

    /// <summary>
    /// Pre-compiled dispatch delegate. Resolves the consumer from the provided <see cref="IServiceProvider"/>,
    /// deserializes the payload, sets the message context, and invokes the correct <c>Consume</c> overload.
    /// Created by source-generated <c>AddConsumer&lt;TConsumer, T&gt;()</c> calls - no runtime reflection required.
    /// </summary>
    public Func<IServiceProvider, string, MessageHeaders, CancellationToken, Task> Dispatch { get; }

    internal ConsumerDescriptor(
        Type consumerType,
        Type messageType,
        string topicName,
        string queueName,
        Func<IServiceProvider, string, MessageHeaders, CancellationToken, Task> dispatch)
    {
        ConsumerType = consumerType;
        MessageType = messageType;
        TopicName = topicName;
        QueueName = queueName;
        Dispatch = dispatch;
    }
}
