using InterBus.Tests.Common;

namespace InterBus.DynamoDB.Tests;

public sealed class OutboxOrderShippedConsumer(OrderShippedTracker tracker) : TrackingConsumer<OrderShipped, OrderShippedTracker>(tracker);
