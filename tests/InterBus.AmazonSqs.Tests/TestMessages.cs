namespace InterBus.AmazonSqs.Tests;

public record OrderPlaced(Guid OrderId, string Email, decimal Amount);
public record OrderShipped(Guid OrderId, string TrackingNumber);
public record InvoiceCreated(Guid InvoiceId, Guid OrderId);
