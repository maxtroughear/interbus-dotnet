using InterBus.Generated;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.PostgreSQL.Tests;

public class PublishSubscribeSingleConsumerTests(string imageTag, PostgreSqlMessagePumpMode pumpMode)
    : PostgreSqlFixtureBase(imageTag, pumpMode,
        bus =>
        {
            bus.Services.AddSingleton<OrderPlacedTracker>();
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
}
