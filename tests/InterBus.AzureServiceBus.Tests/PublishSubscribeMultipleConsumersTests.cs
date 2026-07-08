using InterBus.Generated;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.AzureServiceBus.Tests;

public class PublishSubscribeMultipleConsumersTests() : ServiceBusFixture(bus =>
{
    bus.Services.AddSingleton<OrderPlacedTracker>();
    bus.Services.AddSingleton<AuditTracker>();
    bus.AddConsumer<OrderPlacedConsumer>();
    bus.AddConsumer<AuditConsumer>();
})
{
    private OrderPlacedTracker OrderTracker => Provider.GetRequiredService<OrderPlacedTracker>();
    private AuditTracker AuditTracker => Provider.GetRequiredService<AuditTracker>();

    [Test]
    public async Task Publish_MultipleConsumers_AllReceiveMessage()
    {
        using var scope = Provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
        var msg = new OrderPlaced(Guid.NewGuid(), "fan@out.test", 50m);
        await publisher.PublishAsync(msg, TestContext.CurrentContext.CancellationToken);

        await WaitUntilAsync(() => OrderTracker.Messages.Count == 1 && AuditTracker.Messages.Count == 1);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(OrderTracker.Messages, Has.Count.EqualTo(1));
            Assert.That(msg.OrderId, Is.EqualTo(OrderTracker.Messages.Single().OrderId));
            Assert.That(AuditTracker.Messages, Has.Count.EqualTo(1));
            Assert.That(msg.OrderId, Is.EqualTo(AuditTracker.Messages.Single().OrderId));
        }
    }

    [Test]
    public async Task Send_DirectToQueue_OnlyTargetConsumerReceives()
    {
        using var scope = Provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
        var msg = new OrderPlaced(Guid.NewGuid(), "direct@send.test", 25m);

        await publisher.SendAsync<OrderPlacedConsumer, OrderPlaced>(
            msg, TestContext.CurrentContext.CancellationToken);

        await WaitUntilAsync(() => OrderTracker.Messages.Count == 1);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(OrderTracker.Messages, Has.Count.EqualTo(1));
            Assert.That(msg.OrderId, Is.EqualTo(OrderTracker.Messages.Single().OrderId));
            Assert.That(AuditTracker.Messages, Is.Empty);
        }
    }
}
