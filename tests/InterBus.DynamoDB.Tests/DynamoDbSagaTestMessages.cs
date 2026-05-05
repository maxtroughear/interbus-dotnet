namespace InterBus.DynamoDB.Tests;

public record OrderStarted(Guid OrderId, string CustomerEmail);

public record OrderConfirmed(Guid OrderId);

public record OrderCancelled(Guid OrderId);

