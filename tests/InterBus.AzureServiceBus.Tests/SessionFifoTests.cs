using InterBus.Generated;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.AzureServiceBus.Tests;

/// <summary>
/// Integration tests for Azure Service Bus session-based FIFO processing.
/// Each test provisions a fresh container and service provider to avoid
/// cross-test entity collisions.
/// </summary>
[TestFixture]
public class SessionFifoTests() : ServiceBusFixture(bus =>
{
    bus.Services.AddSingleton<SessionOrderPlacedTracker>();
    bus.Configure<SessionOrderPlaced>(c => c.Fifo(o => o.CustomerId));
    bus.AddConsumer<SessionOrderPlacedConsumer>(c => c.WithFifo());
})
{
    private SessionOrderPlacedTracker Tracker => Provider.GetRequiredService<SessionOrderPlacedTracker>();

    [Test]
    public async Task Publish_FifoConsumer_MessageIsDelivered()
    {
        using var scope = Provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

        var msg = new SessionOrderPlaced(Guid.NewGuid(), "cust-1", 99.99m);
        await publisher.PublishAsync(msg, TestContext.CurrentContext.CancellationToken);

        await WaitUntilAsync(() => Tracker.Messages.Count >= 1, timeoutMs: 30_000);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(Tracker.Messages, Has.Count.EqualTo(1));
            Assert.That(Tracker.Messages.Single().OrderId, Is.EqualTo(msg.OrderId));
        }
    }

    [Test]
    public async Task Publish_FifoConsumer_MultipleGroupsAllDelivered()
    {
        using var scope = Provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

        var messages = Enumerable.Range(1, 5)
            .Select(i => new SessionOrderPlaced(Guid.NewGuid(), $"cust-{i}", i * 10m))
            .ToList();

        foreach (var msg in messages)
            await publisher.PublishAsync(msg, TestContext.CurrentContext.CancellationToken);

        await WaitUntilAsync(() => Tracker.Messages.Count >= messages.Count, timeoutMs: 30_000);

        Assert.That(Tracker.Messages, Has.Count.EqualTo(messages.Count));
        Assert.That(
            Tracker.Messages.Select(m => m.OrderId),
            Is.EquivalentTo(messages.Select(m => m.OrderId)));
    }
}
