namespace InterBus;

/// <summary>
/// Describes a registered consumer and all of the message types it handles.
/// There is exactly one <see cref="ConsumerDescriptor"/> per consumer class, regardless of how
/// many message types that consumer handles.
/// <para>
/// The <see cref="Dispatch"/> delegate is either source-generated (via <c>AddGeneratedConsumers()</c>)
/// or constructed at startup from accumulated per-type handlers. Either way, no runtime reflection
/// is performed during message dispatch.
/// </para>
/// </summary>
public sealed class ConsumerDescriptor
{
    /// <summary>The concrete consumer type.</summary>
    public Type ConsumerType { get; }

    /// <summary>
    /// The queue name for this consumer (derived from the consumer type by convention).
    /// Each consumer class has exactly one queue.
    /// </summary>
    public string QueueName { get; }

    /// <summary>
    /// The topic names this consumer subscribes to — one entry per message type the consumer handles.
    /// The transport subscribes the consumer's queue to each of these topics so published messages
    /// are fan-out delivered to the queue.
    /// </summary>
    public IReadOnlyList<string> TopicNames { get; }

    /// <summary>
    /// Dispatch delegate. Reads the <see cref="InterBusHeaders.MessageType"/> header from the
    /// incoming message, deserializes the payload to the matching type, and invokes the correct
    /// <c>Consume</c> overload on the consumer. If the message type is not handled by this consumer
    /// it logs an error and returns without throwing.
    /// </summary>
    public Func<IServiceProvider, string, MessageHeaders, CancellationToken, Task> Dispatch { get; }

    internal ConsumerDescriptor(
        Type consumerType,
        string queueName,
        IReadOnlyList<string> topicNames,
        Func<IServiceProvider, string, MessageHeaders, CancellationToken, Task> dispatch)
    {
        ConsumerType = consumerType;
        QueueName = queueName;
        TopicNames = topicNames;
        Dispatch = dispatch;
    }
}
