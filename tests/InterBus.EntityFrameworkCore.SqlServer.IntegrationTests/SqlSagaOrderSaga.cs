using InterBus.Sagas;

namespace InterBus.EntityFrameworkCore.SqlServer.IntegrationTests;

public sealed class SqlSagaOrderSaga : ISagaConsumer<SqlSagaOrderState, SqlSagaOrderStarted>, ISagaConsumer<SqlSagaOrderState, SqlSagaOrderConfirmed>
{
    public string GetCorrelationId(SqlSagaOrderStarted message) => message.OrderId.ToString();
    public string GetCorrelationId(SqlSagaOrderConfirmed message) => message.OrderId.ToString();
    public Task Consume(SqlSagaOrderStarted message, ISagaContext<SqlSagaOrderState> context, CancellationToken cancellationToken = default)
    {
        context.State.CustomerEmail = message.CustomerEmail;
        context.State.MessageCount++;
        return Task.CompletedTask;
    }

    public Task Consume(SqlSagaOrderConfirmed message, ISagaContext<SqlSagaOrderState> context, CancellationToken cancellationToken = default)
    {
        context.State.IsConfirmed = true;
        context.State.MessageCount++;
        return Task.CompletedTask;
    }
}
