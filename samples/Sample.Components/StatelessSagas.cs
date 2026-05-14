using InterBus;
using InterBus.Sagas;
using InterBus.Stateless;
using Stateless;

namespace Sample.Components;

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

public record PlaceOrder(Guid OrderId, string CustomerEmail);

public record ConfirmOrder(Guid OrderId);

public record ShipOrder(Guid OrderId, string TrackingNumber);

public record DeliverOrder(Guid OrderId);

public record CancelOrder(Guid OrderId);

// ---------------------------------------------------------------------------
// State machine types
// ---------------------------------------------------------------------------

public enum OrderState { Placed, Confirmed, Shipped, Completed, Cancelled }

public enum OrderTrigger { Confirm, Ship, Deliver, Cancel }

// ---------------------------------------------------------------------------
// Saga state
// ---------------------------------------------------------------------------

public class OrderSagaState : IStateMachineSagaState<OrderState>
{
    public OrderState CurrentState { get; set; } = OrderState.Placed;
    public string? CustomerEmail { get; set; }
    public string? TrackingNumber { get; set; }
}

// ---------------------------------------------------------------------------
// Saga
// ---------------------------------------------------------------------------

/// <summary>
/// Demonstrates a stateless state-machine saga that tracks an order through
/// Placed → Confirmed → Shipped → Completed, with an optional Cancelled path.
/// </summary>
public class OrderSaga(IPublisher publisher)
    : StatelessSaga<OrderSagaState, OrderState, OrderTrigger>,
        ISagaConsumer<OrderSagaState, PlaceOrder>,
        ISagaConsumer<OrderSagaState, ConfirmOrder>,
        ISagaConsumer<OrderSagaState, ShipOrder>,
        ISagaConsumer<OrderSagaState, DeliverOrder>,
        ISagaConsumer<OrderSagaState, CancelOrder>
{
    // ---------------------------------------------------------------------------
    // State machine configuration
    // ---------------------------------------------------------------------------

    protected override void Configure(StateMachine<OrderState, OrderTrigger> machine, OrderSagaState state)
    {
        machine.Configure(OrderState.Placed)
            .Permit(OrderTrigger.Confirm, OrderState.Confirmed)
            .Permit(OrderTrigger.Cancel, OrderState.Cancelled);

        machine.Configure(OrderState.Confirmed)
            .Permit(OrderTrigger.Ship, OrderState.Shipped)
            .Permit(OrderTrigger.Cancel, OrderState.Cancelled);

        machine.Configure(OrderState.Shipped)
            .Permit(OrderTrigger.Deliver, OrderState.Completed);

        machine.Configure(OrderState.Completed)
            .OnEntry(() => Console.WriteLine($"[OrderSaga] Order {state.CustomerEmail} delivered — saga complete"));

        machine.Configure(OrderState.Cancelled)
            .OnEntry(() => Console.WriteLine($"[OrderSaga] Order for {state.CustomerEmail} was cancelled"));
    }

    // ---------------------------------------------------------------------------
    // Correlation
    // ---------------------------------------------------------------------------

    public string GetCorrelationId(PlaceOrder message) => message.OrderId.ToString();
    public string GetCorrelationId(ConfirmOrder message) => message.OrderId.ToString();
    public string GetCorrelationId(ShipOrder message) => message.OrderId.ToString();
    public string GetCorrelationId(DeliverOrder message) => message.OrderId.ToString();
    public string GetCorrelationId(CancelOrder message) => message.OrderId.ToString();

    // ---------------------------------------------------------------------------
    // Handlers
    // ---------------------------------------------------------------------------

    public Task Consume(PlaceOrder message, ISagaContext<OrderSagaState> context,
        CancellationToken cancellationToken = default)
    {
        context.State.CustomerEmail = message.CustomerEmail;
        Console.WriteLine($"[OrderSaga] Order {message.OrderId} placed by {message.CustomerEmail} — state: {context.State.CurrentState}");
        return Task.CompletedTask;
    }

    public async Task Consume(ConfirmOrder message, ISagaContext<OrderSagaState> context,
        CancellationToken cancellationToken = default)
    {
        var machine = CreateStateMachine(context.State);
        await machine.FireAsync(OrderTrigger.Confirm);
        Console.WriteLine($"[OrderSaga] Order {message.OrderId} confirmed — state: {context.State.CurrentState}");

        // Simulate shipping request sent
        await publisher.PublishAsync(new ShipOrder(message.OrderId, $"TRK-{message.OrderId:N}"[..12]),
            cancellationToken);
    }

    public async Task Consume(ShipOrder message, ISagaContext<OrderSagaState> context,
        CancellationToken cancellationToken = default)
    {
        context.State.TrackingNumber = message.TrackingNumber;
        var machine = CreateStateMachine(context.State);
        await machine.FireAsync(OrderTrigger.Ship);
        Console.WriteLine($"[OrderSaga] Order {message.OrderId} shipped ({message.TrackingNumber}) — state: {context.State.CurrentState}");

        // Simulate delivery notification
        await publisher.PublishAsync(new DeliverOrder(message.OrderId), cancellationToken);
    }

    public async Task Consume(DeliverOrder message, ISagaContext<OrderSagaState> context,
        CancellationToken cancellationToken = default)
    {
        var machine = CreateStateMachine(context.State);
        await machine.FireAsync(OrderTrigger.Deliver);
        Console.WriteLine($"[OrderSaga] Order {message.OrderId} delivered — state: {context.State.CurrentState}");
        context.Complete();
    }

    public async Task Consume(CancelOrder message, ISagaContext<OrderSagaState> context,
        CancellationToken cancellationToken = default)
    {
        var machine = CreateStateMachine(context.State);
        await machine.FireAsync(OrderTrigger.Cancel);
        Console.WriteLine($"[OrderSaga] Order {message.OrderId} cancelled — state: {context.State.CurrentState}");
        context.Complete();
    }
}
