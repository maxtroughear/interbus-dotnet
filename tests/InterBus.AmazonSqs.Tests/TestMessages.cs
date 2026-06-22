namespace InterBus.AmazonSqs.Tests;

public record OrderPlaced(Guid OrderId, string Email, decimal Amount);

public record OrderShipped(Guid OrderId, string TrackingNumber);

public record FifoOrderPlaced(Guid OrderId, string CustomerId, decimal Amount);

public record FifoOrderDirect(Guid OrderId, string CustomerId);

public record NestedOrderPlaced(string Value, OrderPlaced Order);
