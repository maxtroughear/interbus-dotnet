using InterBus.Tests.Common;

namespace InterBus.AmazonSqs.Tests;

public sealed class OrderShippedConsumer(OrderShippedTracker tracker) : TrackingConsumer<OrderShipped, OrderShippedTracker>(tracker);
