using InterBus.Tests.Common;

namespace InterBus.DynamoDB.Tests;

public sealed class OutboxOrderPlacedConsumer(OrderPlacedTracker tracker) : TrackingConsumer<OrderPlaced, OrderPlacedTracker>(tracker);
