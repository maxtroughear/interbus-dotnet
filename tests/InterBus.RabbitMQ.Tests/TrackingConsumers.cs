namespace InterBus.RabbitMQ.Tests;

public sealed class OrderPlacedConsumer(OrderPlacedTracker tracker)
    : TrackingConsumer<OrderPlaced, OrderPlacedTracker>(tracker);

public sealed class AuditConsumer(AuditTracker tracker)
    : TrackingConsumer<OrderPlaced, AuditTracker>(tracker);

public sealed class OrderShippedConsumer(OrderShippedTracker tracker)
    : TrackingConsumer<OrderShipped, OrderShippedTracker>(tracker);

public sealed class GetOrderStatusConsumer(IPublisher publisher)
    : RequestReplyConsumer<GetOrderStatusRequest, GetOrderStatusReply>(publisher)
{
    protected override GetOrderStatusReply CreateReply(GetOrderStatusRequest message) =>
        new(message.OrderId, "Shipped");
}
