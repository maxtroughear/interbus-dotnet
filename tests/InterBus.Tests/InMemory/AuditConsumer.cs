namespace InterBus.Tests.InMemory;

public sealed class AuditConsumer(AuditTracker tracker) : TrackingConsumer<OrderPlaced, AuditTracker>(tracker);
