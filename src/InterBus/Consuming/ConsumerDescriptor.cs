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
public sealed class ConsumerDescriptor(Type consumerType, Uri queue)
{
    private static readonly Func<DispatchContext, ConsumePipelineExecutor, IServiceProvider, CancellationToken, Task>
        MissingDispatchDelegate = MissingDispatch;

    private Dictionary<string, Func<DispatchContext, ConsumePipelineExecutor, CancellationToken, Task>> _routes =
        new(StringComparer.Ordinal);

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
    public HashSet<Type> MessageTypes { get; } = [];

    public string MessageTypeHeader { get; internal set; } = InterBusHeaders.MessageType;

    internal Func<DispatchContext, ConsumePipelineExecutor, IServiceProvider, CancellationToken, Task> Dispatch
    {
        get;
        set;
    } = MissingDispatchDelegate;

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

    internal void SetRoute(
        string messageType,
        Func<DispatchContext, ConsumePipelineExecutor, CancellationToken, Task> route)
    {
        _routes[messageType] = route;
    }

    internal void BuildDispatchFromRoutesIfMissing()
    {
        if (!ReferenceEquals(Dispatch, MissingDispatchDelegate))
            return;

        if (_routes.Count == 0)
        {
            throw new InvalidOperationException(
                $"No dispatch routes configured for consumer {ConsumerType.FullName}. " +
                "Call Consumes<TConsumer, TMessage>() or provide Dispatch(...)");
        }

        Dispatch = (context, pipeline, _, cancellationToken) =>
        {
            if (!context.Headers.TryGetValue(MessageTypeHeader, out var msgType))
            {
                throw new InvalidOperationException(
                    $"InterBus: message received without {MessageTypeHeader} header for consumer {ConsumerType.FullName}");
            }

            if (_routes.TryGetValue(msgType, out var route))
                return route(context, pipeline, cancellationToken);

            throw new InvalidOperationException(
                $"InterBus: consumer {ConsumerType.FullName} does not handle message type {msgType}");
        };
    }

    private static Task MissingDispatch(
        DispatchContext context,
        ConsumePipelineExecutor pipeline,
        IServiceProvider serviceProvider,
        CancellationToken cancellationToken) =>
        throw new InvalidOperationException(
            $"No dispatch configured for consumer {context.Descriptor.ConsumerType.FullName}");

    /// <summary>
    /// Creates a <see cref="ConsumerDescriptor"/> for a temporary one-shot reply consumer.
    /// The supplied <paramref name="dispatch"/> delegate is invoked instead of the normal
    /// consumer executor, allowing the caller (e.g. a request client) to complete a
    /// <see cref="System.Threading.Tasks.TaskCompletionSource{T}"/> when the reply arrives.
    /// </summary>
    public static ConsumerDescriptor CreateForReply(
        Type replyConsumerType,
        Uri queue,
        Func<DispatchContext, ConsumePipelineExecutor, IServiceProvider, CancellationToken, Task> dispatch)
    {
        var descriptor = new ConsumerDescriptor(replyConsumerType, queue);
        descriptor.Dispatch = dispatch;
        return descriptor;
    }
}
