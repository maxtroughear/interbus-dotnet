using InterBus.Generated;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.AmazonSqs.Tests;

[TestFixture]
public class ConcurrencyTests() : LocalstackTestFixtureBase(bus =>
{
    bus.Services.AddSingleton(new OrderPlacedTracker());
    bus.AddConsumer<ConcurrencyTestConsumer>(cfg => cfg.WithConcurrency(50).WithPumpCount(5));
})
{
    /// <summary>Number of concurrent publishers used in the concurrency test.</summary>
    private const int PublisherCount = 50;

    /// <summary>Number of messages each publisher sends.</summary>
    private const int MessagesPerPublisher = 1000;

    private OrderPlacedTracker Tracker => Provider.GetRequiredService<OrderPlacedTracker>();

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
                await using var scope = Provider.CreateAsyncScope();
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

        // Wait for all messages to be consumed.
        await WaitUntilAsync(
            () => Tracker.Messages.Count >= totalMessages,
            timeoutMs: 120_000);

        await TestContext.Out.WriteLineAsync(
            $"Consumed {Tracker.Messages.Count}/{totalMessages} messages at {DateTime.UtcNow:O} " +
            $"at a rate of {totalMessages / consumerStopwatch.Elapsed.TotalSeconds:F2}/second");

        Assert.That(Tracker.Messages, Has.Count.EqualTo(totalMessages));
        Assert.That(
            Tracker.Messages.Select(m => m.OrderId),
            Is.EquivalentTo(allMessages.Select(m => m.OrderId)));
    }
}
