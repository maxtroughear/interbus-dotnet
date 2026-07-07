using InterBus.Tests.Common;

namespace InterBus.AmazonSqs.Tests;

public sealed class OrderPlacedTracker : MessageTracker<OrderPlaced>;

public sealed class AuditTracker : MessageTracker<OrderPlaced>;

public abstract class OrderShippedTracker : MessageTracker<OrderShipped>;

public sealed class FifoOrderPlacedTracker : MessageTracker<FifoOrderPlaced>;

public sealed class FifoOrderDirectTracker : MessageTracker<FifoOrderDirect>;

public sealed class OrderPlacedConsumer(OrderPlacedTracker tracker)
    : TrackingConsumer<OrderPlaced, OrderPlacedTracker>(tracker);

public sealed class AuditConsumer(AuditTracker tracker)
    : TrackingConsumer<OrderPlaced, AuditTracker>(tracker);

public sealed class OrderShippedConsumer(OrderShippedTracker tracker)
    : TrackingConsumer<OrderShipped, OrderShippedTracker>(tracker);

public sealed class FifoOrderPlacedConsumer(FifoOrderPlacedTracker tracker)
    : TrackingConsumer<FifoOrderPlaced, FifoOrderPlacedTracker>(tracker);

public sealed class FifoOrderDirectConsumer(FifoOrderDirectTracker tracker)
    : TrackingConsumer<FifoOrderDirect, FifoOrderDirectTracker>(tracker);

public sealed class GetOrderStatusConsumer(IPublisher publisher)
    : RequestReplyConsumer<GetOrderStatusRequest, GetOrderStatusReply>(publisher)
{
    protected override GetOrderStatusReply CreateReply(GetOrderStatusRequest message) =>
        new(message.OrderId, "Shipped");
}
