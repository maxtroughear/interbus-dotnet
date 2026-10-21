using InterBus.Sagas;

namespace InterBus.EntityFrameworkCore.Tests;

public sealed class EfSagaOrderSaga : ISagaConsumer<EfSagaOrderState, EfSagaOrderStarted>, ISagaConsumer<EfSagaOrderState, EfSagaOrderConfirmed>
{
    public string GetCorrelationId(EfSagaOrderStarted message) => message.OrderId.ToString();
    public string GetCorrelationId(EfSagaOrderConfirmed message) => message.OrderId.ToString();
    public Task Consume(EfSagaOrderStarted message, ISagaContext<EfSagaOrderState> context, CancellationToken cancellationToken = default)
    {
        context.State.CustomerEmail = message.CustomerEmail;
        context.State.MessageCount++;
        return Task.CompletedTask;
    }

    public Task Consume(EfSagaOrderConfirmed message, ISagaContext<EfSagaOrderState> context, CancellationToken cancellationToken = default)
    {
        context.State.IsConfirmed = true;
        context.State.MessageCount++;
        return Task.CompletedTask;
    }
}
