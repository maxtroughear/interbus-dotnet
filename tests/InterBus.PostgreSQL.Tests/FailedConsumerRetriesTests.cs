using InterBus.Generated;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace InterBus.PostgreSQL.Tests;

public class FailedConsumerRetriesTests(string imageTag, PostgreSqlMessagePumpMode pumpMode)
    : PostgreSqlFixtureBase(imageTag, pumpMode, bus =>
    {
        bus.Services.AddSingleton<OrderPlacedTracker>();
        bus.AddConsumer<FailingOrderPlacedConsumer>();
    })
{
    private OrderPlacedTracker Tracker => Provider.GetRequiredService<OrderPlacedTracker>();

    [Test]
    public async Task FailedConsumer_MessageIsMovedToDeadLetterTable()
    {
        using var scope = Provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
        var msg = new OrderPlaced(Guid.NewGuid(), "deadletter@test.com", 10m);
        await publisher.PublishAsync(msg, TestContext.CurrentContext.CancellationToken);

        // Wait for the consumer to be called once.
        await WaitUntilAsync(() => Tracker.Messages.Count >= 1, timeoutMs: 15_000);

        // Allow a short window for the pump to write the dead-letter row.
        await Task.Delay(500, TestContext.CurrentContext.CancellationToken);

        var dataSource = scope.ServiceProvider.GetRequiredService<NpgsqlDataSource>();
        var deadLetterCount = await CountDeadLetterRowsAsync(dataSource);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(deadLetterCount, Is.EqualTo(1), "Expected exactly one row in dead_letter_messages");
            Assert.That(Tracker.Messages.Count, Is.EqualTo(1),
                "Failed message should be dead-lettered, not retried");
        }
    }

    private static async Task<int> CountDeadLetterRowsAsync(NpgsqlDataSource dataSource)
    {
        await using var cmd = dataSource.CreateCommand("SELECT COUNT(*) FROM interbus.dead_letter_messages");
        return Convert.ToInt32(await cmd.ExecuteScalarAsync());
    }

    internal sealed class FailingOrderPlacedConsumer(OrderPlacedTracker tracker)
        : TrackingConsumer<OrderPlaced, OrderPlacedTracker>(tracker);
}
