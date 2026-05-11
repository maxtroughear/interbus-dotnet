using InterBus.Dispatching;

namespace InterBus.Consuming;

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
    Uri queue,
    IReadOnlyList<Type> messageTypes,
    Func<DispatchContext, IServiceProvider, CancellationToken, Task> dispatch)
{
    /// <summary>The concrete consumer type.</summary>
    public Type ConsumerType { get; } = consumerType;

    /// <summary>
    /// The queue for this consumer (derived from the consumer type by convention).
    /// Each consumer class has exactly one queue.
    /// </summary>
    public Uri Queue { get; } = queue;

    public string QueueName => RoutingConventions.GetDestinationName(Queue);

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
    internal Func<DispatchContext, IServiceProvider, CancellationToken, Task> Dispatch { get; } = dispatch;

    /// <summary>
    /// The number of concurrent message handlers for this consumer's queue.
    /// Transports that support concurrent dispatch per queue (e.g. RabbitMQ) use this value
    /// to configure the channel's dispatch concurrency and prefetch count.
    /// Defaults to <c>1</c> (sequential dispatch).
    /// </summary>
    public int ConcurrencyCount { get; internal set; } = 1;

    /// <summary>
    /// 
    /// </summary>
    public TimeSpan? Timeout { get; internal set; }
}
