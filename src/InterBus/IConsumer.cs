namespace InterBus;

/// <summary>
/// Marker interface for consumer discovery and registration. Do not implement directly; implement <see cref="IConsumer{T}"/>.
/// </summary>
public interface IConsumer
{
}

/// <summary>
/// Message Endpoint (consumer side). Implement to handle messages of type <typeparamref name="T"/>.
/// Each implementation is activated by the Message Pump when a message is available on the consumer's queue.
/// </summary>
/// <typeparam name="T">The message type to consume.</typeparam>
public interface IConsumer<T> : IConsumer where T : class
{
    Task Consume(T message, IConsumeContext context, CancellationToken cancellationToken = default);
}
