using System.Reflection;
using InterBus.Consuming;
using InterBus.Dispatching;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.Sagas;

/// <summary>
/// Builds the source-generated-equivalent dispatch delegate for a saga at registration time.
/// Reflection is used once during startup; no reflection occurs during message dispatch.
/// </summary>
internal static class SagaDispatchHelper
{
    private static readonly MethodInfo CreateTypedHandlerMethod =
        typeof(SagaDispatchHelper).GetMethod(
            nameof(CreateTypedHandler),
            BindingFlags.Static | BindingFlags.NonPublic)!;

    /// <summary>
    /// Builds the dispatch delegate and the array of handled message types for
    /// <typeparamref name="TSaga"/> / <typeparamref name="TState"/>.
    /// Discovers all <see cref="ISagaConsumer{TState,TMessage}"/> interfaces via reflection.
    /// </summary>
    public static (Type[] MessageTypes, Func<DispatchContext, IServiceProvider, CancellationToken, Task> Dispatch)
        Build<TSaga, TState>(TimeSpan? timeToLive)
        where TSaga : class, ISaga<TState>
        where TState : class, new()
    {
        var sagaType = typeof(TSaga);
        var stateType = typeof(TState);
        var sagaConsumerOpenType = typeof(ISagaConsumer<,>);

        var messageTypes = new List<Type>();
        var handlers = new Dictionary<string, Func<DispatchContext, IServiceProvider, CancellationToken, Task>>(
            StringComparer.Ordinal);

        foreach (var iface in sagaType.GetInterfaces())
        {
            if (!iface.IsGenericType) continue;
            if (iface.GetGenericTypeDefinition() != sagaConsumerOpenType) continue;

            var typeArgs = iface.GetGenericArguments();
            // typeArgs[0] = TState, typeArgs[1] = TMessage
            if (typeArgs[0] != stateType) continue;

            var messageType = typeArgs[1];
            messageTypes.Add(messageType);

            // Build typed handler via reflection: CreateTypedHandler<TSaga, TState, TMessage>(timeToLive)
            var handlerMethod = CreateTypedHandlerMethod.MakeGenericMethod(sagaType, stateType, messageType);
            var handler = (Func<DispatchContext, IServiceProvider, CancellationToken, Task>)
                handlerMethod.Invoke(null, [timeToLive])!;

            handlers[RoutingConventions.TopicName(messageType)] = handler;
        }

        if (messageTypes.Count == 0)
            throw new InvalidOperationException(
                $"Saga '{sagaType.Name}' does not implement ISagaConsumer<{stateType.Name}, TMessage> " +
                "for any message type. Ensure the saga implements at least one " +
                "ISagaConsumer<TState, TMessage> interface.");

        var dispatch = (DispatchContext context, IServiceProvider sp, CancellationToken ct) =>
        {
            if (!context.Headers.TryGetValue(InterBusHeaders.MessageType, out var msgType))
                throw new InvalidOperationException(
                    $"InterBus: message received without {InterBusHeaders.MessageType} header " +
                    $"for saga {sagaType.Name}");

            return handlers.TryGetValue(msgType, out var handler)
                ? handler(context, sp, ct)
                : Task.CompletedTask;
        };

        return (messageTypes.ToArray(), dispatch);
    }

    /// <summary>
    /// Creates a strongly-typed dispatch handler for a single message type.
    /// Called via reflection; constraints ensure correctness for valid saga implementations.
    /// </summary>
    private static Func<DispatchContext, IServiceProvider, CancellationToken, Task>
        CreateTypedHandler<TSaga, TState, TMessage>(TimeSpan? timeToLive)
        where TSaga : class, ISaga<TState>
        where TState : class, new()
        where TMessage : class
    {
        return (context, sp, ct) =>
        {
            var serializer = sp.GetRequiredService<IMessageSerializer>();
            var consumePipeline = sp.GetRequiredService<ConsumePipelineExecutor>();
            var saga = sp.GetRequiredService<TSaga>();
            var sagaConsumer = (ISagaConsumer<TState, TMessage>)saga;
            var lockManager = sp.GetRequiredService<SagaInstanceLockManager>();
            var repository = sp.GetRequiredService<ISagaRepository<TState>>();

            var message = serializer.Deserialize<TMessage>(context.Payload)!;
            var consumeContext = new ConsumeContext<TMessage>(context.Descriptor, message, context.Headers);

            return consumePipeline.ExecuteAsync(
                consumeContext,
                async (msg, ctx, innerCt) =>
                {
                    var correlationId = sagaConsumer.GetCorrelationId(msg);
                    var semaphore = lockManager.GetLock(typeof(TSaga), correlationId);

                    await semaphore.WaitAsync(innerCt);
                    try
                    {
                        var state = await repository.GetAsync(correlationId, innerCt) ?? new TState();
                        var sagaCtx = new SagaContext<TState>(state, ctx);

                        await sagaConsumer.Consume(msg, sagaCtx, innerCt);

                        if (sagaCtx.IsDeleted)
                            await repository.DeleteAsync(correlationId, innerCt);
                        else
                            await repository.SaveAsync(correlationId, sagaCtx.State, timeToLive, innerCt);
                    }
                    finally
                    {
                        semaphore.Release();
                    }
                },
                ct);
        };
    }
}
