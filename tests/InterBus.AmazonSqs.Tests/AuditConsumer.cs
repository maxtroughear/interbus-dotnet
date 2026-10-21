using InterBus.Tests.Common;

namespace InterBus.AmazonSqs.Tests;

public sealed class AuditConsumer(AuditTracker tracker) : TrackingConsumer<OrderPlaced, AuditTracker>(tracker);
