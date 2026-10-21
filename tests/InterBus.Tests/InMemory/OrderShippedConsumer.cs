namespace InterBus.Tests.InMemory;

public sealed class OrderShippedConsumer(OrderShippedTracker tracker) : TrackingConsumer<OrderShipped, OrderShippedTracker>(tracker);
