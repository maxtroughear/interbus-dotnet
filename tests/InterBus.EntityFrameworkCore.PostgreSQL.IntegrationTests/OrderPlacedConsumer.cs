namespace InterBus.EntityFrameworkCore.PostgreSQL.IntegrationTests;

public sealed class OrderPlacedConsumer(OrderPlacedTracker tracker) : TrackingConsumer<OrderPlaced, OrderPlacedTracker>(tracker);
