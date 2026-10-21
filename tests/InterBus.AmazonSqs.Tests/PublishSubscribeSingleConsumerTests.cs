using InterBus.Generated;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.AmazonSqs.Tests;

[TestFixture]
public class PublishSubscribeSingleConsumerTests() : LocalstackTestFixtureBase(bus =>
{
    bus.Services.AddSingleton(new OrderPlacedTracker());
    bus.AddConsumer<OrderPlacedConsumer>();
})
{
    private OrderPlacedTracker OrderTracker => Provider.GetRequiredService<OrderPlacedTracker>();

    [Test]
    public async Task Publish_SingleConsumer_MessageIsDelivered()
    {
        using var scope = Provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
        var msg = new OrderPlaced(Guid.NewGuid(), "test@example.com", 99.99m);
        await publisher.PublishAsync(msg, TestContext.CurrentContext.CancellationToken);

        await WaitUntilAsync(() => OrderTracker.Messages.Count >= 1);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(OrderTracker.Messages, Has.Count.EqualTo(1));
            Assert.That(msg.OrderId, Is.EqualTo(OrderTracker.Messages.Single().OrderId));
        }
    }

    [Test]
    public async Task Send_DirectQueue_MessageIsDelivered()
    {
        using var scope = Provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
        var msg = new OrderPlaced(Guid.NewGuid(), "direct@example.com", 49.99m);
        await publisher.SendAsync<OrderPlacedConsumer, OrderPlaced>(msg, TestContext.CurrentContext.CancellationToken);

        await WaitUntilAsync(() => OrderTracker.Messages.Count >= 1);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(OrderTracker.Messages, Has.Count.EqualTo(1));
            Assert.That(msg.OrderId, Is.EqualTo(OrderTracker.Messages.Single().OrderId));
        }
    }
}
