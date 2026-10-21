using InterBus.Sagas;

namespace InterBus.Tests.InMemory;

public sealed class OrderWithDeletionSaga : ISagaConsumer<OrderSagaState, OrderStarted>, ISagaConsumer<OrderSagaState, OrderCancelled>
{
    public string GetCorrelationId(OrderStarted message) => message.OrderId.ToString();
    public string GetCorrelationId(OrderCancelled message) => message.OrderId.ToString();
    public Task Consume(OrderStarted message, ISagaContext<OrderSagaState> context, CancellationToken cancellationToken = default)
    {
        context.State.CustomerEmail = message.CustomerEmail;
        context.State.MessageCount++;
        return Task.CompletedTask;
    }

    public Task Consume(OrderCancelled message, ISagaContext<OrderSagaState> context, CancellationToken cancellationToken = default)
    {
        context.State.IsCancelled = true;
        context.Delete();
        return Task.CompletedTask;
    }
}
