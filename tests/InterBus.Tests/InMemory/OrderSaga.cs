using InterBus.Sagas;

namespace InterBus.Tests.InMemory;

public sealed class OrderSaga : ISagaConsumer<OrderSagaState, OrderStarted>, ISagaConsumer<OrderSagaState, OrderConfirmed>
{
    public string GetCorrelationId(OrderStarted message) => message.OrderId.ToString();
    public string GetCorrelationId(OrderConfirmed message) => message.OrderId.ToString();
    public Task Consume(OrderStarted message, ISagaContext<OrderSagaState> context, CancellationToken cancellationToken = default)
    {
        context.State.CustomerEmail = message.CustomerEmail;
        context.State.MessageCount++;
        return Task.CompletedTask;
    }

    public Task Consume(OrderConfirmed message, ISagaContext<OrderSagaState> context, CancellationToken cancellationToken = default)
    {
        context.State.IsConfirmed = true;
        context.State.MessageCount++;
        return Task.CompletedTask;
    }
}
