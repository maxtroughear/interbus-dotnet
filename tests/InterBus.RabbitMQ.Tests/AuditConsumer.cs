namespace InterBus.RabbitMQ.Tests;

public sealed class AuditConsumer(AuditTracker tracker) : TrackingConsumer<OrderPlaced, AuditTracker>(tracker);
