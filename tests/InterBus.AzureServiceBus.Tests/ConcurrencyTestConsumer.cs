namespace InterBus.AzureServiceBus.Tests;

public sealed class ConcurrencyTestConsumer(OrderPlacedTracker tracker) : TrackingConsumer<OrderPlaced, OrderPlacedTracker>(tracker);
