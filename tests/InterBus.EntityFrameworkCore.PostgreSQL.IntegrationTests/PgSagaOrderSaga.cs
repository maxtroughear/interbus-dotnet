using InterBus.Sagas;

namespace InterBus.EntityFrameworkCore.PostgreSQL.IntegrationTests;

public sealed class PgSagaOrderSaga : ISagaConsumer<PgSagaOrderState, PgSagaOrderStarted>, ISagaConsumer<PgSagaOrderState, PgSagaOrderConfirmed>
{
    public string GetCorrelationId(PgSagaOrderStarted message) => message.OrderId.ToString();
    public string GetCorrelationId(PgSagaOrderConfirmed message) => message.OrderId.ToString();
    public Task Consume(PgSagaOrderStarted message, ISagaContext<PgSagaOrderState> context, CancellationToken cancellationToken = default)
    {
        context.State.CustomerEmail = message.CustomerEmail;
        context.State.MessageCount++;
        return Task.CompletedTask;
    }

    public Task Consume(PgSagaOrderConfirmed message, ISagaContext<PgSagaOrderState> context, CancellationToken cancellationToken = default)
    {
        context.State.IsConfirmed = true;
        context.State.MessageCount++;
        return Task.CompletedTask;
    }
}
