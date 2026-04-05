using System.Collections.Concurrent;

namespace InterBus.EntityFrameworkCore.SqlServer.IntegrationTests;

public sealed class OrderPlacedTracker
{
    private readonly ConcurrentBag<OrderPlaced> _messages = [];
    public IReadOnlyCollection<OrderPlaced> Messages => _messages;
    public void Track(OrderPlaced message) => _messages.Add(message);
}

public sealed class OrderPlacedConsumer(OrderPlacedTracker tracker) : IConsumer<OrderPlaced>
{
    public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        tracker.Track(message);
        return Task.CompletedTask;
    }
}
