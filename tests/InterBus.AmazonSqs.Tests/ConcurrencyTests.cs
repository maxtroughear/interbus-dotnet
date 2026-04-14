using System.Diagnostics;
using Amazon;
using Amazon.Extensions.NETCore.Setup;
using Amazon.Runtime;
using InterBus.AmazonSqs.Extensions;
using InterBus.Extensions;
using InterBus.Generated;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Testcontainers.LocalStack;

namespace InterBus.AmazonSqs.Tests;

[TestFixture]
public class ConcurrencyTests
{
    private static readonly LocalStackContainer Container =
        new LocalStackBuilder("localstack/localstack:4").Build();

    private ServiceProvider _provider;
    private CancellationTokenSource _cts;
    private OrderPlacedTracker _orderTracker;
    private string _runId = null!;

    /// <summary>Number of concurrent publishers used in the concurrency test.</summary>
    private const int PublisherCount = 5;

    /// <summary>Number of messages each publisher sends.</summary>
    private const int MessagesPerPublisher = 1000;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        await Container.StartAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await Container.StopAsync();
        await Container.DisposeAsync();
    }

    [SetUp]
    public async Task Setup()
    {
        _runId = Guid.NewGuid().ToString("N")[..8];
        _orderTracker = new OrderPlacedTracker();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(_orderTracker);

        // Register AWS clients pointing at the LocalStack container.
        var localStackOptions = new AWSOptions
        {
            Credentials = new BasicAWSCredentials("test", "test"),
            Region = RegionEndpoint.USEast1,
            DefaultClientConfig =
            {
                ServiceURL = Container.GetConnectionString()
            }
        };
        services.AddAWSService<Amazon.SimpleNotificationService.IAmazonSimpleNotificationService>(localStackOptions);
        services.AddAWSService<Amazon.SQS.IAmazonSQS>(localStackOptions);

        services.AddInterBus(bus =>
        {
            bus.AddAmazonSqsTransport(o =>
            {
                o.QueuePrefix = $"q-{_runId}-";
                o.TopicPrefix = $"t-{_runId}-";
            });
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
        var totalMessages = PublisherCount * MessagesPerPublisher;

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
                var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();

                foreach (var message in batch)
                {
                    if (ct.IsCancellationRequested)
                        break;

                    await publisher.Publish(message, ct);
                }

                await TestContext.Out.WriteLineAsync(
                    $"Published batch of {batch.Length} messages at {DateTime.UtcNow:O} " +
                    $"at a rate of {batch.Length / batchStopwatch.Elapsed.TotalSeconds:F2}/second");
            });

        // Wait for all messages to be consumed.
        await TestHelpers.WaitUntilAsync(
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
