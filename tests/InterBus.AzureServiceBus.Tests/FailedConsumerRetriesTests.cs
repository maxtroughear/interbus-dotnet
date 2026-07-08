using InterBus.AzureServiceBus.Extensions;
using InterBus.Extensions;
using InterBus.Generated;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace InterBus.AzureServiceBus.Tests;

public class FailedConsumerRetriesTests : ServiceBusFixture
{
    private ServiceProvider _provider;
    private CancellationTokenSource _cts;
    private OrderPlacedTracker _tracker;
    private ReceiveTracker _receiveTracker;

    [SetUp]
    public async Task Setup()
    {
        _tracker = new OrderPlacedTracker();
        _receiveTracker = new ReceiveTracker();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(_tracker);
        services.AddSingleton(_receiveTracker);
        services.AddInterBus(bus =>
        {
            bus.AddAzureServiceBusTransport(o =>
            {
                o.ConnectionString = Container.GetConnectionString();
                o.AdminConnectionString = Container.GetHttpConnectionString();
            });
            bus.AddConsumer<DeadLetterOrderPlacedConsumer>();
        });

        _provider = services.BuildServiceProvider();
        _cts = new CancellationTokenSource();

        foreach (var svc in _provider.GetServices<IHostedService>())
            await svc.StartAsync(_cts.Token);
    }

    [TearDown]
    public async Task TearDown()
    {
        await _cts.CancelAsync();

        foreach (var svc in _provider.GetServices<IHostedService>())
            await svc.StopAsync(CancellationToken.None);

        _cts.Dispose();
        await _provider.DisposeAsync();
    }

    [Test]
    public async Task FailedConsumer_MessageIsMovedToDeadLetterQueue()
    {
        using var scope = _provider.CreateScope();
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
        Assert.That(_tracker.Messages, Is.Empty);
        Assert.That(_receiveTracker.Count, Is.EqualTo(1));
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
