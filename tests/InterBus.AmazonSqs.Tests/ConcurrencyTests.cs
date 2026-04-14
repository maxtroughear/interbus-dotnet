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
        new LocalStackBuilder("localstack/localstack:3.8.1").Build();

    private ServiceProvider _provider;
    private CancellationTokenSource _cts;
    private OrderPlacedTracker _orderTracker;
    private string _runId = null!;

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

        var consumerStopwatch = Stopwatch.StartNew();
        
        var taskA = Task.Run(async () =>
        {
            var stopwatch = Stopwatch.StartNew();
            foreach (var message in messagesA)
            {
                if (TestContext.CurrentContext.CancellationToken.IsCancellationRequested)
                    break;

                await publisherA.Publish(message, TestContext.CurrentContext.CancellationToken);
            }

            await TestContext.Out.WriteLineAsync($"A: Published {messagesA.Length} messages at {DateTime.UtcNow:O} at a rate of {messagesA.Length / stopwatch.Elapsed.TotalSeconds:F2}/second");
        });

        var taskB = Task.Run(async () =>
        {
            var stopwatch = Stopwatch.StartNew();
            foreach (var message in messagesB)
            {
                if (TestContext.CurrentContext.CancellationToken.IsCancellationRequested)
                    break;

                await publisherB.Publish(message, TestContext.CurrentContext.CancellationToken);
            }

            await TestContext.Out.WriteLineAsync($"B: Published {messagesB.Length} messages at {DateTime.UtcNow:O} at a rate of {messagesA.Length / stopwatch.Elapsed.TotalSeconds:F2}/second");
        });

        var taskC = Task.Run(async () =>
        {
            var stopwatch = Stopwatch.StartNew();
            foreach (var message in messagesC)
            {
                if (TestContext.CurrentContext.CancellationToken.IsCancellationRequested)
                    break;

                await publisherC.Publish(message, TestContext.CurrentContext.CancellationToken);
            }

            await TestContext.Out.WriteLineAsync($"C: Published {messagesC.Length} messages at {DateTime.UtcNow:O} at a rate of {messagesA.Length / stopwatch.Elapsed.TotalSeconds:F2}/second");
        });

        await Task.WhenAll(taskA, taskB, taskC);

        var allMessages = messagesA.Concat(messagesB).Concat(messagesC).ToList();

        // Wait for all messages to be consumed
        await TestHelpers.WaitUntilAsync(() => _orderTracker.Messages.Count >= allMessages.Count,
            timeoutMs: 60_000);

        await TestContext.Out.WriteLineAsync(
            $"Consumed {_orderTracker.Messages.Count} messages at {DateTime.UtcNow:O} at a rate of {allMessages.Count / consumerStopwatch.Elapsed.TotalSeconds:F2}/second");

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
