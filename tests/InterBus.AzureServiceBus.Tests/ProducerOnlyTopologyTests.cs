using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.AzureServiceBus.Tests;

public class ProducerOnlyTopologyTests() : ServiceBusFixture(_ => { })
{
    
    [Test]
    public async Task Publish_NoConsumers_TopicIsEnsured()
    {
        using var scope = Provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

        var msg = new OrderPlaced(Guid.NewGuid(), "publish-only@test.com", 42m);
        await publisher.PublishAsync(msg, TestContext.CurrentContext.CancellationToken);

        var expectedTopicName = RoutingConventions.TopicName(typeof(OrderPlaced));
        var adminClient = new ServiceBusAdministrationClient(Container.GetHttpConnectionString());

        var exists = await adminClient.TopicExistsAsync(
            expectedTopicName,
            TestContext.CurrentContext.CancellationToken);

        Assert.That(exists.Value, Is.True,
            $"Expected topic '{expectedTopicName}' to exist after publish");
    }

    [Test]
    public async Task Send_NoConsumers_DoesNotEnsureQueue()
    {
        using var scope = Provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

        const string rawQueueName = "direct-no-consumer";
        var expectedQueueName = rawQueueName;

        var msg = new OrderPlaced(Guid.NewGuid(), "queue-only@test.com", 7m);
        Assert.That(
            async () => await publisher.SendAsync(rawQueueName, msg, TestContext.CurrentContext.CancellationToken),
            Throws.TypeOf<ServiceBusException>());

        var adminClient = new ServiceBusAdministrationClient(Container.GetHttpConnectionString());
        var exists = await adminClient.QueueExistsAsync(
            expectedQueueName,
            TestContext.CurrentContext.CancellationToken);

        Assert.That(exists.Value, Is.False,
            $"Expected queue '{expectedQueueName}' not to be created by send");
    }
}
