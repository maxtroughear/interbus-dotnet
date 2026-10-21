using InterBus.Generated;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.AmazonSqs.Tests;

[TestFixture]
public partial class DeadLetteringTests() : LocalstackTestFixtureBase(bus =>
{
    bus.Services.AddSingleton(new ReceiveTracker());
    bus.AddConsumer<DeadLetterOrderPlacedConsumer>();
})
{
    private ReceiveTracker Tracker => Provider.GetRequiredService<ReceiveTracker>();

    [Test]
    public async Task FailedConsumer_MessageIsMovedToDeadLetterQueueWithMetadata()
    {
        using var scope = Provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
        await publisher.PublishAsync(
            new OrderPlaced(Guid.NewGuid(), "deadletter@test.com", 1m),
            TestContext.CurrentContext.CancellationToken);

        var consumerDescriptor = scope.ServiceProvider.GetRequiredService<InterBusOptions>().Consumers
            .First(cd => cd.ConsumerType == typeof(DeadLetterOrderPlacedConsumer));
        var deadLetterQueueName = scope.ServiceProvider.GetRequiredService<SnsSqsNaming>()
            .BuildDeadLetterQueueName(consumerDescriptor.Queue);
        var deadLetterQueueUrl = await scope.ServiceProvider.GetRequiredService<SnsSqsTopologyBuilder>()
            .ResolveQueueAsync(deadLetterQueueName, TestContext.CurrentContext.CancellationToken);

        var sqsClient = Provider.GetRequiredService<IAmazonSQS>();
        Message? deadLetterMessage = null;
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (deadLetterMessage is null && DateTime.UtcNow < deadline)
        {
            if (deadLetterQueueUrl is not null)
            {
                var response = await sqsClient.ReceiveMessageAsync(
                    new ReceiveMessageRequest
                    {
                        QueueUrl = deadLetterQueueUrl,
                        MaxNumberOfMessages = 1,
                        WaitTimeSeconds = 1,
                        MessageAttributeNames = ["All"],
                    });

                deadLetterMessage = (response.Messages ?? [])
                    .FirstOrDefault(message => message.MessageAttributes.ContainsKey(InterBusHeaders.DeadLetterReason));
            }

            if (deadLetterMessage is null)
                await Task.Delay(200, TestContext.CurrentContext.CancellationToken);
        }

        Assert.That(deadLetterMessage, Is.Not.Null);
        Assert.That(deadLetterMessage!.MessageAttributes.ContainsKey(InterBusHeaders.DeadLetterReason), Is.True);
        Assert.That(deadLetterMessage.MessageAttributes.ContainsKey(InterBusHeaders.DeadLetterError), Is.True);
        Assert.That(deadLetterMessage.MessageAttributes.ContainsKey(InterBusHeaders.DeadLetterTime), Is.True);
        Assert.That(Tracker.Count, Is.EqualTo(1));
    }
}
