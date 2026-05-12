namespace InterBus;

public interface IConsume<in T>;

/// <summary>
/// Marker interface for consumer discovery and registration. Do not implement directly; implement <see cref="IConsumer{T}"/>.
/// </summary>
public interface IConsumer;

/// <summary>
/// Message Endpoint (consumer side). Implement to handle messages of type <typeparamref name="TMessage"/>.
/// Each implementation is activated by the Message Pump when a message is available on the consumer's queue.
/// </summary>
/// <typeparam name="TMessage">The message type to consume.</typeparam>
public interface IConsumer<in TMessage> : IConsumer, IConsume<TMessage>
    where TMessage : class
{
    Task Consume(TMessage message, IConsumeContext context, CancellationToken cancellationToken = default);
}
