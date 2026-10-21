using InterBus.Tests.Common;

namespace InterBus.AmazonSqs.Tests;

public sealed class OrderPlacedConsumer(OrderPlacedTracker tracker) : TrackingConsumer<OrderPlaced, OrderPlacedTracker>(tracker);
