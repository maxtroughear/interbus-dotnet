using InterBus.Sagas;

namespace InterBus.Tests.InMemory;

public sealed class AcknowledgingOrderSaga(IPublisher publisher) : ISagaConsumer<OrderSagaState, OrderStarted>
{
    public string GetCorrelationId(OrderStarted message) => message.OrderId.ToString();
    public async Task Consume(OrderStarted message, ISagaContext<OrderSagaState> context, CancellationToken cancellationToken = default)
    {
        context.State.CustomerEmail = message.CustomerEmail;
        await publisher.PublishAsync(new OrderAcknowledged(message.OrderId), cancellationToken);
    }
}
