using System.Diagnostics;
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

    private const int PublisherCount = 100;
    private const int MessagesPerPublisher = 100;
    private const int ConsumerCount = 50;

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
            bus.AddConsumer<ConcurrencyTestConsumer>(cfg => cfg.WithConcurrency(ConsumerCount));
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
        const int totalMessages = PublisherCount * MessagesPerPublisher;

        // Partition messages across N publishers so the test is scalable: change
        // PublisherCount / MessagesPerPublisher at the top of the class to adjust load.
        var allMessages = Enumerable.Range(0, totalMessages)
            .Select(i => new OrderPlaced(Guid.CreateVersion7(), $"test{i}@test.com", i))
            .ToArray();

        var batches = allMessages.Chunk(MessagesPerPublisher).ToArray();

        var consumerStopwatch = Stopwatch.StartNew();

        // Publish all batches in parallel, each in its own DI scope, so that each
        // "publisher" is fully independent without requiring manual A/B/C variables.
        await Parallel.ForEachAsync(
            batches,
            new ParallelOptions
            {
                MaxDegreeOfParallelism = PublisherCount,
                CancellationToken = TestContext.CurrentContext.CancellationToken,
            },
            async (batch, ct) =>
            {
                var batchStopwatch = Stopwatch.StartNew();
                await using var scope = _provider.CreateAsyncScope();
                var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

                foreach (var message in batch)
                {
                    if (ct.IsCancellationRequested)
                        break;

                    await publisher.PublishAsync(message, ct);
                }

                await TestContext.Out.WriteLineAsync(
                    $"Published batch of {batch.Length} messages at {DateTime.UtcNow:O} " +
                    $"at a rate of {batch.Length / batchStopwatch.Elapsed.TotalSeconds:F2}/second");
            });
        await TestContext.Out.WriteLineAsync($"All {totalMessages} messages published at {DateTime.UtcNow:O} " +
                                             $"at an overall rate of {totalMessages / consumerStopwatch.Elapsed.TotalSeconds:F2}/second");

        // Wait for all messages to be consumed.
        await WaitUntilAsync(
            () => _orderTracker.Messages.Count >= totalMessages,
            timeoutMs: 120_000);

        await TestContext.Out.WriteLineAsync(
            $"Consumed {_orderTracker.Messages.Count}/{totalMessages} messages at {DateTime.UtcNow:O} " +
            $"at a rate of {totalMessages / consumerStopwatch.Elapsed.TotalSeconds:F2}/second");

        Assert.That(_orderTracker.Messages, Has.Count.EqualTo(totalMessages));
        Assert.That(
            _orderTracker.Messages.Select(m => m.OrderId),
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
