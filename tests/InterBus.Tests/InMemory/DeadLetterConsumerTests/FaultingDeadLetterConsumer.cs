using InterBus.DeadLettering;

namespace InterBus.Tests.InMemory;

public partial class DeadLetterConsumerTests
{
    /// <summary>Consumer of <see cref = "DeadLetter{T}"/> that itself always throws.</summary>
    public sealed class FaultingDeadLetterConsumer(FaultingConsumerCallCount count) : IConsumer<DeadLetter<OrderPlaced>>
    {
        public Task Consume(DeadLetter<OrderPlaced> message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            count.Increment();
            throw new InvalidOperationException("Intentional dead-letter consumer failure");
        }
    }
}
