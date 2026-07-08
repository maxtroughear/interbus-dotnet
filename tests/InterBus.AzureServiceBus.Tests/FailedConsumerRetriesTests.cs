using InterBus.Generated;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.AzureServiceBus.Tests;

public class FailedConsumerRetriesTests() : ServiceBusFixture(bus =>
{
    bus.Services.AddSingleton<OrderPlacedTracker>();
    bus.Services.AddSingleton<ReceiveTracker>();
    bus.AddConsumer<DeadLetterOrderPlacedConsumer>();
})
{
    private OrderPlacedTracker Tracker => Provider.GetRequiredService<OrderPlacedTracker>();
    private ReceiveTracker ReceiveTracker => Provider.GetRequiredService<ReceiveTracker>();
    
    [Test]
    public async Task FailedConsumer_MessageIsMovedToDeadLetterQueue()
    {
        using var scope = Provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
        var msg = new OrderPlaced(Guid.NewGuid(), "retry@test.com", 10m);
        await publisher.PublishAsync(msg, TestContext.CurrentContext.CancellationToken);

        ServiceBusReceivedMessage? deadLetterMessage = null;
        var deadline = DateTime.UtcNow.AddSeconds(60);
        while (deadLetterMessage is null && DateTime.UtcNow < deadline)
        {
            deadLetterMessage = await TryReadDeadLetterAsync();
            if (deadLetterMessage is null)
                await Task.Delay(200, TestContext.CurrentContext.CancellationToken);
        }

        Assert.That(deadLetterMessage, Is.Not.Null);
        Assert.That(deadLetterMessage!.DeadLetterReason, Is.EqualTo("InvalidOperationException"));
        Assert.That(deadLetterMessage.ApplicationProperties.ContainsKey(InterBusHeaders.DeadLetterError), Is.True);
        Assert.That(deadLetterMessage.ApplicationProperties.ContainsKey(InterBusHeaders.DeadLetterTime), Is.True);
        Assert.That(Tracker.Messages, Is.Empty);
        Assert.That(ReceiveTracker.Count, Is.EqualTo(1));
    }

    private async Task<ServiceBusReceivedMessage?> TryReadDeadLetterAsync()
    {
        var queueName = typeof(DeadLetterOrderPlacedConsumer).FullName!;
        await using var client = new ServiceBusClient(Container.GetConnectionString());
        await using var receiver = client.CreateReceiver(
            queueName,
            new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
        return await receiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(100));
    }
}

/// <summary>Tracks how many times a consumer was invoked regardless of outcome.</summary>
public sealed class ReceiveTracker
{
    private int _count;
    public int Count => _count;
    public void Increment() => Interlocked.Increment(ref _count);
}

public sealed class DeadLetterOrderPlacedConsumer(ReceiveTracker receiveTracker)
    : IConsumer<OrderPlaced>
{
    public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        receiveTracker.Increment();
        throw new InvalidOperationException("Simulated dead-letter failure");
    }
}
