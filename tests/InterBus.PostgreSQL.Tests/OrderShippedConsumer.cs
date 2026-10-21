namespace InterBus.PostgreSQL.Tests;

public sealed class OrderShippedConsumer(OrderShippedTracker tracker) : TrackingConsumer<OrderShipped, OrderShippedTracker>(tracker);
