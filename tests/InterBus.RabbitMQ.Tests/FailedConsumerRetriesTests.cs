using InterBus.Extensions;
using InterBus.Generated;
using InterBus.RabbitMQ.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using Testcontainers.RabbitMq;

namespace InterBus.RabbitMQ.Tests;

[TestFixtureSource(typeof(RabbitMqVersions), nameof(RabbitMqVersions.All))]
public class FailedConsumerRetriesTests(string imageTag)
{
    private readonly RabbitMqContainer _container = new RabbitMqBuilder(imageTag).Build();
    private ServiceProvider _provider;
    private CancellationTokenSource _cts;
    private OrderPlacedTracker _tracker;
    private ReceiveTracker _receiveTracker;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        await _container.StartAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _container.StopAsync();
        await _container.DisposeAsync();
    }

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
            bus.AddRabbitMqTransport(o => { o.Uri = _container.GetConnectionString(); });
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

        var deadLetterQueueName = $"{typeof(DeadLetterOrderPlacedConsumer).FullName}.dlq";

        byte[]? deadLetterBody = null;
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (deadLetterBody is null && DateTime.UtcNow < deadline)
        {
            deadLetterBody = await TryReadDeadLetterAsync(deadLetterQueueName);
            if (deadLetterBody is null)
                await Task.Delay(200, TestContext.CurrentContext.CancellationToken);
        }

        Assert.That(deadLetterBody, Is.Not.Null);
        Assert.That(_tracker.Messages, Is.Empty);
        Assert.That(_receiveTracker.Count, Is.EqualTo(1));
    }

    private async Task<byte[]?> TryReadDeadLetterAsync(string deadLetterQueueName)
    {
        var factory = new ConnectionFactory { Uri = new Uri(_container.GetConnectionString()) };
        await using var connection = await factory.CreateConnectionAsync();
        await using var channel = await connection.CreateChannelAsync();
        var message = await channel.BasicGetAsync(deadLetterQueueName, autoAck: true);
        return message?.Body.ToArray();
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
