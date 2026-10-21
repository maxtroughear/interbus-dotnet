using System.Collections.Concurrent;

namespace InterBus.Tests.InMemory;

public partial class HeadersTests
{
    public sealed class CorrelationIdCapturingConsumer(ConcurrentBag<string?> correlationIds) : IConsumer<OrderPlaced>
    {
        public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            correlationIds.Add(context.CorrelationId);
            return Task.CompletedTask;
        }
    }
}
