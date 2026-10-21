namespace InterBus.EntityFrameworkCore.SqlServer.IntegrationTests;

public sealed class OrderPlacedConsumer(OrderPlacedTracker tracker) : TrackingConsumer<OrderPlaced, OrderPlacedTracker>(tracker);
