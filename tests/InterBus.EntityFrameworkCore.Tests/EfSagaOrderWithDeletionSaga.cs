using InterBus.Sagas;

namespace InterBus.EntityFrameworkCore.Tests;

public sealed class EfSagaOrderWithDeletionSaga : ISagaConsumer<EfSagaOrderState, EfSagaOrderStarted>, ISagaConsumer<EfSagaOrderState, EfSagaOrderCancelled>
{
    public string GetCorrelationId(EfSagaOrderStarted message) => message.OrderId.ToString();
    public string GetCorrelationId(EfSagaOrderCancelled message) => message.OrderId.ToString();
    public Task Consume(EfSagaOrderStarted message, ISagaContext<EfSagaOrderState> context, CancellationToken cancellationToken = default)
    {
        context.State.CustomerEmail = message.CustomerEmail;
        context.State.MessageCount++;
        return Task.CompletedTask;
    }

    public Task Consume(EfSagaOrderCancelled message, ISagaContext<EfSagaOrderState> context, CancellationToken cancellationToken = default)
    {
        context.State.IsCancelled = true;
        context.Delete();
        return Task.CompletedTask;
    }
}
