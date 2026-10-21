using InterBus.DeadLettering;

namespace InterBus.Tests.InMemory;

public partial class DeadLetterConsumerTests
{
    /// <summary>Tracks nested dead letters (should never be received).</summary>
    public sealed class NestedDeadLetterTracker : MessageTracker<DeadLetter<DeadLetter<OrderPlaced>>>;
}
