using System.Diagnostics;
using InterBus.Generated;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.PostgreSQL.Tests;

[Category("Load")]
[CancelAfter(600_000)]
public class ConcurrencyTests(string imageTag, PostgreSqlMessagePumpMode pumpMode) : PostgreSqlFixtureBase(imageTag,
    pumpMode, bus =>
    {
        bus.Services.AddSingleton<OrderPlacedTracker>();
        bus.AddConsumer<ConcurrencyTestConsumer>(cfg => cfg.WithConcurrency(ConsumerCount)
            .WithPumpCount(pumpMode == PostgreSqlMessagePumpMode.Lease ? 4 : 1));
    })
{
    private OrderPlacedTracker OrderTracker => Provider.GetRequiredService<OrderPlacedTracker>();

    private const int PublisherCount = 100;
    private const int MessagesPerPublisher = 100;
    private const int ConsumerCount = 50;

    [Test]
    public async Task LotsOfPublishedMessages_AllAreConsumed()
    {
        const int totalMessages = PublisherCount * MessagesPerPublisher;
        await TestContext.Progress.WriteLineAsync($"Starting load test: {imageTag}, {pumpMode}, {totalMessages} messages");
        await using var progress = new PostgreSqlLoadProgress(Container.GetConnectionString(), () => OrderTracker.Messages.Count);

        // Partition messages across N publishers, so the test is scalable: change
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
                    await publisher.PublishAsync(message, ct);
                    progress.Published();
                }

                await TestContext.Out.WriteLineAsync(
                    $"Published batch of {batch.Length} messages at {DateTime.UtcNow:O} " +
                    $"at a rate of {batch.Length / batchStopwatch.Elapsed.TotalSeconds:F2}/second");
            });
        await TestContext.Out.WriteLineAsync($"All {totalMessages} messages published at {DateTime.UtcNow:O} " +
                                             $"at an overall rate of {totalMessages / consumerStopwatch.Elapsed.TotalSeconds:F2}/second");

        // Wait for all messages to be consumed.
        await WaitUntilAsync(
            () => OrderTracker.Messages.Count >= totalMessages,
            timeoutMs: 120_000,
            cancellationToken: TestContext.CurrentContext.CancellationToken);

        var received = OrderTracker.Messages.Count;
        await TestContext.Progress.WriteLineAsync(
            $"Consumed {received}/{totalMessages} messages at {DateTime.UtcNow:O} " +
            $"at a rate of {received / consumerStopwatch.Elapsed.TotalSeconds:F2}/second");

        Assert.That(OrderTracker.Messages, Has.Count.EqualTo(totalMessages));
        Assert.That(
            OrderTracker.Messages.Select(m => m.OrderId),
            Is.EquivalentTo(allMessages.Select(m => m.OrderId)));
    }

    public sealed class ConcurrencyTestConsumer(OrderPlacedTracker tracker) : IConsumer<OrderPlaced>
    {
        public Task Consume(OrderPlaced message, IConsumeContext context,
            CancellationToken cancellationToken = default)
        {
            tracker.Track(message);
            return Task.CompletedTask;
        }
    }
}
