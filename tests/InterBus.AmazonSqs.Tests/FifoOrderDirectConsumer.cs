using InterBus.Tests.Common;

namespace InterBus.AmazonSqs.Tests;

public sealed class FifoOrderDirectConsumer(FifoOrderDirectTracker tracker) : TrackingConsumer<FifoOrderDirect, FifoOrderDirectTracker>(tracker);
