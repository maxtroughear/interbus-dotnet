using InterBus.Generated;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace InterBus.PostgreSQL.Tests;

public class DeadLetteringTests(string imageTag, PostgreSqlMessagePumpMode pumpMode) : PostgreSqlFixtureBase(imageTag,
    pumpMode, bus =>
    {
        bus.Services.AddSingleton<OrderPlacedTracker>();
        bus.AddConsumer<DeadLetterOrderPlacedConsumer>();
    })
{
    [Test]
    public async Task FailedConsumer_MessageIsMovedToDeadLetterTable()
    {
        var tracker = Provider.GetRequiredService<OrderPlacedTracker>();

        using var scope = Provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
        var msg = new OrderPlaced(Guid.NewGuid(), "dlq-lease@test.com", 1m);
        await publisher.PublishAsync(msg, TestContext.CurrentContext.CancellationToken);

        // Wait until the consumer has been called.
        await WaitUntilAsync(() => tracker.Messages.Count >= 1, timeoutMs: 15_000);

        // Allow a short window for the pump to write to dead_letter_messages.
        await Task.Delay(500, TestContext.CurrentContext.CancellationToken);

        var row = await ReadDeadLetterRowAsync();

        using (Assert.EnterMultipleScope())
        {
            Assert.That(row, Is.Not.Null, "Expected a row in dead_letter_messages");
            Assert.That(row!.QueueName, Does.Contain(nameof(DeadLetterOrderPlacedConsumer)));
            Assert.That(row.Reason, Is.EqualTo("InvalidOperationException"));
            Assert.That(row.Error, Does.Contain("Simulated dead-letter failure"));

            if (PumpMode == PostgreSqlMessagePumpMode.Lease)
                Assert.That(row.DeliveryCount, Is.EqualTo(1));

            Assert.That(row.DeadLetteredAt,
                Is.LessThanOrEqualTo(DateTimeOffset.UtcNow).And.GreaterThan(DateTimeOffset.MinValue));
            Assert.That(tracker.Messages, Has.Count.EqualTo(1),
                "Message should be dead-lettered on the first attempt, not retried");
        }
    }

    private async Task<DeadLetterRow?> ReadDeadLetterRowAsync()
    {
        using var scope = Provider.CreateScope();
        var dataSource = scope.ServiceProvider.GetRequiredService<NpgsqlDataSource>();
        await using var cmd = dataSource.CreateCommand(
            "SELECT queue_name, reason, error, dead_lettered_at, delivery_count FROM interbus.dead_letter_messages LIMIT 1");
        await using var reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
            return null;

        return new DeadLetterRow(
            reader.GetString(0),
            reader.GetString(1),
            reader.GetString(2),
            reader.GetFieldValue<DateTimeOffset>(3),
            reader.GetInt32(4));
    }

    public sealed class DeadLetterOrderPlacedConsumer(OrderPlacedTracker tracker) : IConsumer<OrderPlaced>
    {
        public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            tracker.Track(message);
            throw new InvalidOperationException("Simulated dead-letter failure");
        }
    }

    private sealed record DeadLetterRow(
        string QueueName,
        string Reason,
        string Error,
        DateTimeOffset DeadLetteredAt,
        int DeliveryCount);
}
