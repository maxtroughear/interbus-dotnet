using InterBus.Middleware;

namespace InterBus;

/// <summary>
/// Describes a registered consumer and all of the message types it handles.
/// There is exactly one <see cref="ConsumerDescriptor"/> per consumer class, regardless of how
/// many message types that consumer handles.
/// <para>
/// The <see cref="Dispatch"/> delegate is either source-generated (via <c>AddConsumers()</c>)
/// or constructed at startup from accumulated per-type handlers. Either way, no runtime reflection
/// is performed during message dispatch.
/// </para>
/// </summary>
public sealed class ConsumerDescriptor(
    Type consumerType,
    string queueName,
    IReadOnlyList<Type> messageTypes,
    Func<DispatchContext, IServiceProvider, CancellationToken, Task> dispatch)
{
    /// <summary>The concrete consumer type.</summary>
    public Type ConsumerType { get; } = consumerType;

    /// <summary>
    /// The queue name for this consumer (derived from the consumer type by convention).
    /// Each consumer class has exactly one queue.
    /// </summary>
    public string QueueName { get; } = queueName;

    /// <summary>
    /// The message types this consumer handles
    /// </summary>
    public IReadOnlyList<Type> MessageTypes { get; } = messageTypes;

    /// <summary>
    /// Dispatch delegate. Reads the <see cref="InterBusHeaders.MessageType"/> header from the
    /// incoming <see cref="IConsumeContext"/>, deserializes the payload to the matching type,
    /// and invokes the correct <c>Consume</c> overload on the consumer. If the message type is
    /// not handled by this consumer it logs an error and returns without throwing.
    /// </summary>
    public Func<DispatchContext, IServiceProvider, CancellationToken, Task> Dispatch { get; } = dispatch;
}
