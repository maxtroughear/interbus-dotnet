using InterBus.Sagas;

namespace InterBus.DynamoDB.Tests;

public sealed class OrderSagaState
{
    public string? CustomerEmail { get; set; }
    public bool IsConfirmed { get; set; }
    public bool IsCancelled { get; set; }
    public int MessageCount { get; set; }
}

public sealed class OrderSaga :
    ISagaConsumer<OrderSagaState, OrderStarted>,
    ISagaConsumer<OrderSagaState, OrderConfirmed>
{
    public string GetCorrelationId(OrderStarted message) => message.OrderId.ToString();
    public string GetCorrelationId(OrderConfirmed message) => message.OrderId.ToString();

    public Task Consume(OrderStarted message, ISagaContext<OrderSagaState> context,
        CancellationToken cancellationToken = default)
    {
        context.State.CustomerEmail = message.CustomerEmail;
        context.State.MessageCount++;
        return Task.CompletedTask;
    }

    public Task Consume(OrderConfirmed message, ISagaContext<OrderSagaState> context,
        CancellationToken cancellationToken = default)
    {
        context.State.IsConfirmed = true;
        context.State.MessageCount++;
        return Task.CompletedTask;
    }
}

public sealed class OrderWithDeletionSaga :
    ISagaConsumer<OrderSagaState, OrderStarted>,
    ISagaConsumer<OrderSagaState, OrderCancelled>
{
    public string GetCorrelationId(OrderStarted message) => message.OrderId.ToString();
    public string GetCorrelationId(OrderCancelled message) => message.OrderId.ToString();

    public Task Consume(OrderStarted message, ISagaContext<OrderSagaState> context,
        CancellationToken cancellationToken = default)
    {
        context.State.CustomerEmail = message.CustomerEmail;
        context.State.MessageCount++;
        return Task.CompletedTask;
    }

    public Task Consume(OrderCancelled message, ISagaContext<OrderSagaState> context,
        CancellationToken cancellationToken = default)
    {
        context.State.IsCancelled = true;
        context.Delete();
        return Task.CompletedTask;
    }
}

public sealed class CompletingOrderSaga :
    ISagaConsumer<OrderSagaState, OrderStarted>,
    ISagaConsumer<OrderSagaState, OrderConfirmed>
{
    public string GetCorrelationId(OrderStarted message) => message.OrderId.ToString();
    public string GetCorrelationId(OrderConfirmed message) => message.OrderId.ToString();

    public Task Consume(OrderStarted message, ISagaContext<OrderSagaState> context,
        CancellationToken cancellationToken = default)
    {
        context.State.CustomerEmail = message.CustomerEmail;
        context.State.MessageCount++;
        return Task.CompletedTask;
    }

    public Task Consume(OrderConfirmed message, ISagaContext<OrderSagaState> context,
        CancellationToken cancellationToken = default)
    {
        context.State.MessageCount++;
        context.Complete();
        return Task.CompletedTask;
    }
}
