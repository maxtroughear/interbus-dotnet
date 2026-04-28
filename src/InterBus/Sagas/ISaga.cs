namespace InterBus.Sagas;

/// <summary>
/// Marker interface for saga discovery and registration.
/// Do not implement directly; implement <see cref="ISagaConsumer{TState,TMessage}"/>.
/// Extends <see cref="IConsumer"/> so that sagas can be registered with the consumer infrastructure.
/// </summary>
public interface ISaga : IConsumer;

/// <summary>
/// Marker interface for a saga that uses <typeparamref name="TState"/> as its shared state type.
/// Each saga class has exactly one state type. Implement <see cref="ISagaConsumer{TState,TMessage}"/>
/// for each message type the saga handles.
/// </summary>
/// <typeparam name="TState">The saga state type. Must be a class with a public parameterless constructor.</typeparam>
public interface ISaga<TState> : ISaga where TState : class, new();
