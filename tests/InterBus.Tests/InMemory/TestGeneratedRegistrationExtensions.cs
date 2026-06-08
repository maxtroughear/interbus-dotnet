using InterBus.Consuming;
using InterBus.Sagas;

namespace InterBus.Generated;

internal static class TestGeneratedRegistrationExtensions
{
    public static InterBusBuilder AddConsumer<TConsumer>(
        this InterBusBuilder builder,
        Action<ConsumerBuilder<TConsumer>>? configure = null)
        where TConsumer : class, IConsumer
        => builder.RegisterConsumer<TConsumer>(consumerBuilder =>
        {
            RegisterConsumerMessageTypes<TConsumer>(consumerBuilder);
            configure?.Invoke(consumerBuilder);
        });

    public static InterBusBuilder AddSaga<TSaga, TState>(
        this InterBusBuilder builder,
        Action<SagaBuilder<TSaga, TState>>? configure = null)
        where TSaga : class, ISaga<TState>
        where TState : class, new()
        => builder.RegisterSaga<TSaga, TState>(sagaBuilder =>
        {
            RegisterSagaMessageTypes<TSaga, TState>(sagaBuilder);
            configure?.Invoke(sagaBuilder);
        });

    private static void RegisterConsumerMessageTypes<TConsumer>(ConsumerBuilder<TConsumer> builder)
        where TConsumer : class, IConsumer
    {
        var consumesMethod = typeof(ConsumerBuilder<TConsumer>).GetMethods()
            .Single(m =>
                m.Name == nameof(ConsumerBuilder<TConsumer>.Consumes) &&
                m.DeclaringType == typeof(ConsumerBuilder<TConsumer>) &&
                m.IsGenericMethodDefinition &&
                m.GetParameters().Length == 0);

        foreach (var messageType in GetConsumerMessageTypes(typeof(TConsumer)))
            consumesMethod.MakeGenericMethod(typeof(TConsumer), messageType).Invoke(builder, null);
    }

    private static void RegisterSagaMessageTypes<TSaga, TState>(SagaBuilder<TSaga, TState> builder)
        where TSaga : class, ISaga<TState>
        where TState : class, new()
    {
        var consumesMethod = typeof(SagaBuilder<TSaga, TState>).GetMethods()
            .Single(m =>
                m.Name == nameof(SagaBuilder<TSaga, TState>.Consumes) &&
                m.DeclaringType == typeof(SagaBuilder<TSaga, TState>) &&
                m.IsGenericMethodDefinition &&
                m.GetParameters().Length == 0);

        foreach (var messageType in GetConsumerMessageTypes(typeof(TSaga)))
            consumesMethod.MakeGenericMethod(typeof(TSaga), messageType).Invoke(builder, null);
    }

    private static IEnumerable<Type> GetConsumerMessageTypes(Type consumerType) =>
        consumerType
            .GetInterfaces()
            .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IConsume<>))
            .Select(i => i.GetGenericArguments()[0])
            .Distinct();
}
