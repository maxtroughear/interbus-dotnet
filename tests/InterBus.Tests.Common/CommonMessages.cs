namespace InterBus.Tests.Common;

public record OrderPlaced(Guid OrderId, string CustomerEmail, decimal Total);

public record OrderShipped(Guid OrderId, string TrackingNumber);

public record GetOrderStatusRequest(Guid OrderId);

public record GetOrderStatusReply(Guid OrderId, string Status);

public record SequencedOrder(string MessageGroupId, int Sequence);
