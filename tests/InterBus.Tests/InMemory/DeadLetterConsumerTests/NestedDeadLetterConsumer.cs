using InterBus.DeadLettering;

namespace InterBus.Tests.InMemory;

public partial class DeadLetterConsumerTests
{
    /// <summary>Consumer of <c>DeadLetter&lt;DeadLetter&lt;OrderPlaced&gt; &gt;</c> - should never be invoked.</summary>
    public sealed class NestedDeadLetterConsumer(NestedDeadLetterTracker tracker) : IConsumer<DeadLetter<DeadLetter<OrderPlaced>>>
    {
        public Task Consume(DeadLetter<DeadLetter<OrderPlaced>> message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            tracker.Track(message);
            return Task.CompletedTask;
        }
    }
}
