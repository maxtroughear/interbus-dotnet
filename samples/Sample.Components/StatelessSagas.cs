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

public enum OrderState
{
    Pending,
    Placed,
    Confirmed,
    Shipped,
    Completed,
    Cancelled
}

public enum OrderTrigger
{
    Place,
    Confirm,
    Ship,
    Deliver,
    Cancel
}

// ---------------------------------------------------------------------------
// Saga state
// ---------------------------------------------------------------------------

public class OrderSagaState : IStateMachineSagaState<OrderState>
{
    public OrderState CurrentState { get; set; } = OrderState.Pending;
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

    private StateMachine<OrderState, OrderTrigger>.TriggerWithParameters<PlaceOrder>? _placedTrigger;
    private StateMachine<OrderState, OrderTrigger>.TriggerWithParameters<ConfirmOrder>? _confirmTrigger;
    private StateMachine<OrderState, OrderTrigger>.TriggerWithParameters<ShipOrder>? _shipTrigger;

    protected override void Configure(StateMachine<OrderState, OrderTrigger> machine,
        ISagaContext<OrderSagaState> context)
    {
        _placedTrigger = machine.SetTriggerParameters<PlaceOrder>(OrderTrigger.Place);
        _confirmTrigger = machine.SetTriggerParameters<ConfirmOrder>(OrderTrigger.Confirm);
        _shipTrigger = machine.SetTriggerParameters<ShipOrder>(OrderTrigger.Ship);

        machine.Configure(OrderState.Pending)
            .Permit(OrderTrigger.Place, OrderState.Placed);

        machine.Configure(OrderState.Placed)
            .OnEntryFrom(_placedTrigger, order => context.State.CustomerEmail = order.CustomerEmail)
            .Permit(OrderTrigger.Confirm, OrderState.Confirmed)
            .Permit(OrderTrigger.Cancel, OrderState.Cancelled);

        machine.Configure(OrderState.Confirmed)
            .OnEntryFromAsync(_confirmTrigger,
                order => publisher.PublishAsync(new ShipOrder(order.OrderId, $"TRK-{order.OrderId:N}"[..12])))
            .Permit(OrderTrigger.Ship, OrderState.Shipped)
            .Permit(OrderTrigger.Cancel, OrderState.Cancelled);

        machine.Configure(OrderState.Shipped)
            .OnEntryFromAsync(_shipTrigger, order =>
            {
                context.State.TrackingNumber = order.TrackingNumber;
                return publisher.PublishAsync(new DeliverOrder(order.OrderId));
            })
            .Permit(OrderTrigger.Deliver, OrderState.Completed);

        machine.Configure(OrderState.Completed)
            .OnEntry(() =>
                Console.WriteLine($"[OrderSaga] Order {context.State.CustomerEmail} delivered — saga complete"));

        machine.Configure(OrderState.Cancelled)
            .OnEntry(() => Console.WriteLine($"[OrderSaga] Order for {context.State.CustomerEmail} was cancelled"));
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

    public async Task Consume(PlaceOrder message, ISagaContext<OrderSagaState> context,
        CancellationToken cancellationToken = default)
    {
        var machine = await CreateStateMachineAsync(context);
        await machine.FireAsync(_placedTrigger, message);
        Console.WriteLine(
            $"[OrderSaga] Order {message.OrderId} placed by {message.CustomerEmail} — state: {context.State.CurrentState}");
    }

    public async Task Consume(ConfirmOrder message, ISagaContext<OrderSagaState> context,
        CancellationToken cancellationToken = default)
    {
        var machine = await CreateStateMachineAsync(context);
        await machine.FireAsync(_confirmTrigger, message);
        Console.WriteLine($"[OrderSaga] Order {message.OrderId} confirmed — state: {context.State.CurrentState}");
    }

    public async Task Consume(ShipOrder message, ISagaContext<OrderSagaState> context,
        CancellationToken cancellationToken = default)
    {
        var machine = await CreateStateMachineAsync(context);
        await machine.FireAsync(_shipTrigger, message);
        Console.WriteLine(
            $"[OrderSaga] Order {message.OrderId} shipped ({message.TrackingNumber}) — state: {context.State.CurrentState}");
    }

    public async Task Consume(DeliverOrder message, ISagaContext<OrderSagaState> context,
        CancellationToken cancellationToken = default)
    {
        var machine = await CreateStateMachineAsync(context);
        await machine.FireAsync(OrderTrigger.Deliver);
        Console.WriteLine($"[OrderSaga] Order {message.OrderId} delivered — state: {context.State.CurrentState}");
        context.Complete();
    }

    public async Task Consume(CancelOrder message, ISagaContext<OrderSagaState> context,
        CancellationToken cancellationToken = default)
    {
        var machine = await CreateStateMachineAsync(context);
        await machine.FireAsync(OrderTrigger.Cancel);
        Console.WriteLine($"[OrderSaga] Order {message.OrderId} cancelled — state: {context.State.CurrentState}");
        context.Complete();
    }
}
