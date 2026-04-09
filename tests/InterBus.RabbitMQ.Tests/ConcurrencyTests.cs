using InterBus.Extensions;
using InterBus.Generated;
using InterBus.RabbitMQ.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Testcontainers.RabbitMq;

namespace InterBus.RabbitMQ.Tests;

[TestFixtureSource(typeof(RabbitMqVersions), nameof(RabbitMqVersions.All))]
public class ConcurrencyTests(string imageTag)
{
    private readonly RabbitMqContainer _container = new RabbitMqBuilder(imageTag).Build();
    private ServiceProvider _provider;
    private CancellationTokenSource _cts;
    private OrderPlacedTracker _orderTracker;

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
        _orderTracker = new OrderPlacedTracker();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(_orderTracker);
        services.AddInterBus(bus =>
        {
            bus.AddRabbitMqTransport(o => { o.Uri = _container.GetConnectionString(); });
            bus.AddConsumer<ConcurrencyTestConsumer>(cfg => cfg.WithConcurrency(20));
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
    public async Task LotsOfPublishedMessages_AllAreConsumed()
    {
        using var scopeA = _provider.CreateScope();
        var publisherA = scopeA.ServiceProvider.GetRequiredService<IMessagePublisher>();
        var messagesA = Enumerable.Range(0, 10000)
            .Select(i => new OrderPlaced(Guid.CreateVersion7(), $"testA{i}@test.com", i)).ToArray();

        using var scopeB = _provider.CreateScope();
        var publisherB = scopeB.ServiceProvider.GetRequiredService<IMessagePublisher>();
        var messagesB = Enumerable.Range(0, 10000)
            .Select(i => new OrderPlaced(Guid.CreateVersion7(), $"testB{i}@test.com", i)).ToArray();

        using var scopeC = _provider.CreateScope();
        var publisherC = scopeC.ServiceProvider.GetRequiredService<IMessagePublisher>();
        var messagesC = Enumerable.Range(0, 10000)
            .Select(i => new OrderPlaced(Guid.CreateVersion7(), $"testC{i}@test.com", i)).ToArray();

        var taskA = Task.Run(async () =>
        {
            foreach (var message in messagesA)
            {
                if (TestContext.CurrentContext.CancellationToken.IsCancellationRequested)
                    break;

                await publisherA.Publish(message, TestContext.CurrentContext.CancellationToken);
            }

            await TestContext.Out.WriteLineAsync($"A: Published {messagesA.Length} messages at {DateTime.UtcNow:O}");
        });

        var taskB = Task.Run(async () =>
        {
            foreach (var message in messagesB)
            {
                if (TestContext.CurrentContext.CancellationToken.IsCancellationRequested)
                    break;

                await publisherB.Publish(message, TestContext.CurrentContext.CancellationToken);
            }

            await TestContext.Out.WriteLineAsync($"B: Published {messagesB.Length} messages at {DateTime.UtcNow:O}");
        });

        var taskC = Task.Run(async () =>
        {
            foreach (var message in messagesC)
            {
                if (TestContext.CurrentContext.CancellationToken.IsCancellationRequested)
                    break;

                await publisherC.Publish(message, TestContext.CurrentContext.CancellationToken);
            }

            await TestContext.Out.WriteLineAsync($"C: Published {messagesC.Length} messages at {DateTime.UtcNow:O}");
        });

        await Task.WhenAll(taskA, taskB, taskC);

        var allMessages = messagesA.Concat(messagesB).Concat(messagesC).ToList();

        // Wait for all messages to be consumed
        await TestHelpers.WaitUntilAsync(() => _orderTracker.Messages.Count >= allMessages.Count,
            timeoutMs: 60_000);

        await TestContext.Out.WriteLineAsync(
            $"Consumed {_orderTracker.Messages.Count} messages at {DateTime.UtcNow:O}");

        Assert.That(_orderTracker.Messages, Has.Count.EqualTo(allMessages.Count));
        Assert.That(_orderTracker.Messages.Select(m => m.OrderId),
            Is.EquivalentTo(allMessages.Select(m => m.OrderId)));
    }
}

public sealed class ConcurrencyTestConsumer(OrderPlacedTracker tracker) : IConsumer<OrderPlaced>
{
    public async Task Consume(OrderPlaced message, IConsumeContext context,
        CancellationToken cancellationToken = default)
    {
        tracker.Track(message);
        await Task.Delay(Random.Shared.Next(1, 5), cancellationToken); // Simulate some work
    }
}
