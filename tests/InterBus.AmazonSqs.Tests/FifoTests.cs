using Amazon.SimpleNotificationService;
using Amazon.SQS;
using Amazon.SQS.Model;
using InterBus.Generated;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.AmazonSqs.Tests;

[TestFixture]
public class FifoTests() : LocalstackTestFixtureBase(bus =>
{
    bus.Configure<FifoOrderDirect>(c => c.Fifo(o => o.CustomerId));
    bus.Configure<FifoOrderPlaced>(c => c.Fifo(o => o.CustomerId));
    bus.Services.AddSingleton(new FifoOrderPlacedTracker());
    bus.AddConsumer<FifoOrderPlacedConsumer>(c => c.WithFifo());
})
{
    private FifoOrderPlacedTracker Tracker => Provider.GetRequiredService<FifoOrderPlacedTracker>();

    [Test]
    public async Task Publish_FifoTopic_MessageDeliveredToFifoQueue()
    {
        using var scope = Provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

        var msg = new FifoOrderPlaced(Guid.NewGuid(), "cust-1", 99.99m);
        await publisher.PublishAsync(msg, TestContext.CurrentContext.CancellationToken);

        await WaitUntilAsync(() => Tracker.Messages.Count >= 1);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(Tracker.Messages, Has.Count.EqualTo(1));
            Assert.That(Tracker.Messages.Single().OrderId, Is.EqualTo(msg.OrderId));
        }
    }

    [Test]
    public async Task Publish_FifoTopic_MultipleGroupsDeliveredInOrder()
    {
        using var scope = Provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

        var orders = Enumerable.Range(1, 5)
            .Select(i => new FifoOrderPlaced(Guid.NewGuid(), $"cust-{i}", i * 10m))
            .ToList();

        foreach (var order in orders)
            await publisher.PublishAsync(order, TestContext.CurrentContext.CancellationToken);

        await WaitUntilAsync(() => Tracker.Messages.Count >= orders.Count);

        Assert.That(Tracker.Messages, Has.Count.EqualTo(orders.Count));
        Assert.That(Tracker.Messages.Select(m => m.OrderId),
            Is.EquivalentTo(orders.Select(o => o.OrderId)));
    }

    [Test]
    public async Task Send_FifoQueue_MessageDeliveredDirectly()
    {
        using var scope = Provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

        var msg = new FifoOrderPlaced(Guid.NewGuid(), "cust-42", 0);
        await publisher.SendAsync<FifoOrderPlacedConsumer, FifoOrderPlaced>(
            msg, TestContext.CurrentContext.CancellationToken);

        await WaitUntilAsync(() => Tracker.Messages.Count >= 1);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(Tracker.Messages, Has.Count.EqualTo(1));
            Assert.That(Tracker.Messages.Single().OrderId, Is.EqualTo(msg.OrderId));
        }
    }

    [Test]
    public async Task Send_ExternalFifoQueue_UsesGroupByMessageGroupId()
    {
        var sqsClient = Provider.GetRequiredService<IAmazonSQS>();
        var externalQueueName = $"{QueuePrefix}external-target.fifo";
        var interbusQueue = new Uri("sqsqueue://interbus/external-target?fifo=true");
        var createResponse = await sqsClient.CreateQueueAsync(
            new CreateQueueRequest
            {
                QueueName = externalQueueName,
                Attributes = new Dictionary<string, string>
                {
                    ["FifoQueue"] = "true",
                    ["ContentBasedDeduplication"] = "true",
                }
            },
            TestContext.CurrentContext.CancellationToken);

        using var scope = Provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
        var msg = new FifoOrderDirect(Guid.NewGuid(), "cust-external-42");
        await publisher.SendAsync(interbusQueue, msg, TestContext.CurrentContext.CancellationToken);

        ReceiveMessageResponse? receive = null;
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (DateTime.UtcNow < deadline)
        {
            receive = await sqsClient.ReceiveMessageAsync(
                new ReceiveMessageRequest
                {
                    QueueUrl = createResponse.QueueUrl,
                    MaxNumberOfMessages = 1,
                    WaitTimeSeconds = 1,
                    MessageSystemAttributeNames = ["All"]
                },
                TestContext.CurrentContext.CancellationToken);

            if (receive.Messages.Count > 0)
                break;
        }

        Assert.That(receive, Is.Not.Null);
        Assert.That(receive!.Messages, Has.Count.EqualTo(1));
        Assert.That(
            receive.Messages[0].Attributes.TryGetValue("MessageGroupId", out var messageGroupId)
                ? messageGroupId
                : null,
            Is.EqualTo("cust-external-42"));
    }

    [Test]
    public async Task Publish_FifoTopic_TopicAndQueueNameHaveFifoSuffix()
    {
        var snsClient = Provider.GetRequiredService<IAmazonSimpleNotificationService>();
        var sqsClient = Provider.GetRequiredService<IAmazonSQS>();

        // Trigger topology creation by publishing
        using var scope = Provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
        await publisher.PublishAsync(
            new FifoOrderPlaced(Guid.NewGuid(), "cust-1", 1m),
            TestContext.CurrentContext.CancellationToken);

        await WaitUntilAsync(() => Tracker.Messages.Count >= 1);

        // Verify SNS topic has .fifo suffix
        var topics = await snsClient.ListTopicsAsync(TestContext.CurrentContext.CancellationToken);
        var fifoTopic = topics.Topics.FirstOrDefault(t =>
            t.TopicArn.Contains(TopicPrefix, StringComparison.Ordinal) &&
            t.TopicArn.EndsWith(".fifo", StringComparison.Ordinal));
        Assert.That(fifoTopic, Is.Not.Null, "Expected a .fifo SNS topic to be created");

        // Verify SQS queue has .fifo suffix
        var queues = await sqsClient.ListQueuesAsync(QueuePrefix, TestContext.CurrentContext.CancellationToken);
        var fifoQueue = queues.QueueUrls.FirstOrDefault(u =>
            u.EndsWith(".fifo", StringComparison.Ordinal));
        Assert.That(fifoQueue, Is.Not.Null, "Expected a .fifo SQS queue to be created");
    }
}
