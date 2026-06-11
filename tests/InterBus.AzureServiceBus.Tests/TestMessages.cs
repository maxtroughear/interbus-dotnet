namespace InterBus.AzureServiceBus.Tests;

public record OrderPlaced(Guid OrderId, string CustomerEmail, decimal Total);

public record OrderShipped(Guid OrderId, string TrackingNumber);

// ── Session / FIFO ──────────────────────────────────────────────────────────

public record SessionOrderPlaced(Guid OrderId, string CustomerId, decimal Total);

// ── Request / Reply ─────────────────────────────────────────────────────────

public record GetOrderStatusRequest(Guid OrderId);

public record GetOrderStatusReply(Guid OrderId, string Status);
