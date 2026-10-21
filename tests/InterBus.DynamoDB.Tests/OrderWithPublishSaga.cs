using InterBus.Sagas;

namespace InterBus.DynamoDB.Tests;

public sealed class OrderWithPublishSaga(IPublisher publisher) : ISagaConsumer<OrderSagaState, OrderStarted>, ISagaConsumer<OrderSagaState, OrderConfirmed>
{
    public string GetCorrelationId(OrderStarted message) => message.OrderId.ToString();
    public string GetCorrelationId(OrderConfirmed message) => message.OrderId.ToString();
    public Task Consume(OrderStarted message, ISagaContext<OrderSagaState> context, CancellationToken cancellationToken = default)
    {
        context.State.CustomerEmail = message.CustomerEmail;
        context.State.MessageCount++;
        return publisher.PublishAsync(new OrderConfirmed(message.OrderId), cancellationToken);
    }

    public Task Consume(OrderConfirmed message, ISagaContext<OrderSagaState> context, CancellationToken cancellationToken = default)
    {
        context.State.IsConfirmed = true;
        context.State.MessageCount++;
        return Task.CompletedTask;
    }
}
