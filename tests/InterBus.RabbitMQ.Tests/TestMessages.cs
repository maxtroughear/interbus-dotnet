namespace InterBus.RabbitMQ.Tests;

public record OrderPlaced(Guid OrderId, string CustomerEmail, decimal Total);

public record OrderShipped(Guid OrderId, string TrackingNumber);
