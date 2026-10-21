namespace InterBus.PostgreSQL.Tests;

public sealed class OrderPlacedConsumer(OrderPlacedTracker tracker) : TrackingConsumer<OrderPlaced, OrderPlacedTracker>(tracker);
