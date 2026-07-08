using InterBus.Generated;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.AmazonSqs.Tests;

[TestFixture]
public class PublishSubscribeMultipleConsumersTests() : LocalstackTestFixtureBase(bus =>
{
    bus.Services.AddSingleton(new OrderPlacedTracker());
    bus.Services.AddSingleton(new AuditTracker());

    bus.AddConsumer<OrderPlacedConsumer>();
    bus.AddConsumer<AuditConsumer>();
})
{
    private OrderPlacedTracker OrderTracker => Provider.GetRequiredService<OrderPlacedTracker>();
    private AuditTracker AuditTracker => Provider.GetRequiredService<AuditTracker>();

    [Test]
    public async Task Publish_MultipleConsumers_BothReceiveMessage()
    {
        using var scope = Provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
        var msg = new OrderPlaced(Guid.NewGuid(), "fan@example.com", 150.00m);
        await publisher.PublishAsync(msg, TestContext.CurrentContext.CancellationToken);

        await WaitUntilAsync(() => OrderTracker.Messages.Count >= 1 && AuditTracker.Messages.Count >= 1);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(OrderTracker.Messages, Has.Count.EqualTo(1));
            Assert.That(AuditTracker.Messages, Has.Count.EqualTo(1));
            Assert.That(msg.OrderId, Is.EqualTo(OrderTracker.Messages.Single().OrderId));
            Assert.That(msg.OrderId, Is.EqualTo(AuditTracker.Messages.Single().OrderId));
        }
    }
}
