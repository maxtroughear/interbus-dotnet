using InterBus.Tests.Common;

namespace InterBus.AmazonSqs.Tests;

public sealed class FifoOrderPlacedConsumer(FifoOrderPlacedTracker tracker) : TrackingConsumer<FifoOrderPlaced, FifoOrderPlacedTracker>(tracker);
