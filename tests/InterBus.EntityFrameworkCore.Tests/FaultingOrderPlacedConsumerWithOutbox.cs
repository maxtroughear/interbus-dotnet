namespace InterBus.EntityFrameworkCore.Tests;

/// <summary>
/// A consumer that always throws, used to verify dead-letter delivery when the outbox is configured.
/// Uses a distinct name to avoid conflicts with other test projects.
/// </summary>
public sealed class FaultingOrderPlacedConsumerWithOutbox : IConsumer<OrderPlaced>
{
    public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default) => throw new InvalidOperationException("Intentional failure for dead-letter outbox test");
}
