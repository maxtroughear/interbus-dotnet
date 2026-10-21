using InterBus.Generated;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using System.Collections.Concurrent;
using InterBus.PostgreSQL.Extensions;

namespace InterBus.PostgreSQL.Tests;

[TestFixtureSource(typeof(PostgreSqlVersions), nameof(PostgreSqlVersions.Transactional))]
public class TransactionContextTests(string imageTag, PostgreSqlMessagePumpMode pumpMode) : PostgreSqlFixtureBase(
    imageTag, pumpMode, bus =>
    {
        bus.Services.AddSingleton<Transactions>();
        bus.AddConsumer<TransactionConsumer>(consumer => consumer.WithConcurrency(2));
    })
{
    [Test]
    public async Task Transactional_Consumer_CanAccessSharedTransaction()
    {
        await PublishAsync();
        var state = Provider.GetRequiredService<Transactions>();
        await WaitUntilAsync(() => state.Seen.Count == 1);
        Assert.That(state.Seen.Single().Value, Is.EqualTo(42));
    }

    [Test]
    public async Task Transactional_EachDispatch_ReceivesNonNullTransaction()
    {
        var state = Provider.GetRequiredService<Transactions>();
        for (var i = 0; i < 6; i++) await PublishAsync();
        await WaitUntilAsync(() => state.Seen.Count == 6);
        Assert.That(state.Seen.Select(item => item.TransactionId).Distinct().Count(), Is.EqualTo(6));
        Assert.That(state.Seen.All(item => item.Value == 42), Is.True);
    }

    [Test]
    public async Task Transactional_ReleaseRollsBackConsumerChanges()
    {
        var dataSource = Provider.GetRequiredService<NpgsqlDataSource>();
        await using (var setup = dataSource.CreateCommand("CREATE TABLE IF NOT EXISTS test_ledger (value int); TRUNCATE test_ledger"))
            await setup.ExecuteNonQueryAsync();
        var state = Provider.GetRequiredService<Transactions>();
        state.UseLedger = true;
        await PublishAsync();
        await WaitUntilAsync(async () =>
        {
            await using var count = dataSource.CreateCommand("SELECT COUNT(*) FROM test_ledger");
            return (long)(await count.ExecuteScalarAsync())! == 1;
        }, cancellationToken: TestContext.CurrentContext.CancellationToken);
        Assert.That(state.Attempts, Is.EqualTo(2));
    }

    private async Task PublishAsync()
    {
        await using var scope = Provider.CreateAsyncScope();
        await scope.ServiceProvider.GetRequiredService<IPublisher>()
            .PublishAsync(new OrderPlaced(Guid.NewGuid(), "transaction@test.com", 42));
    }

    public sealed class Transactions
    {
        public bool UseLedger;
        public int Attempts;
        public ConcurrentBag<(long TransactionId, int Value)> Seen { get; } = [];
    }
    public sealed class TransactionConsumer(Transactions state) : IConsumer<OrderPlaced>
    {
        public async Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            var transaction = context.GetPostgreSqlTransaction()
                ?? throw new InvalidOperationException("Missing ambient transaction");
            await using var command = transaction.Connection!.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "SELECT txid_current()";
            var transactionId = (long)(await command.ExecuteScalarAsync(cancellationToken))!;
            command.CommandText = "SELECT 42";
            var value = (int)(await command.ExecuteScalarAsync(cancellationToken))!;
            if (state.UseLedger)
            {
                command.CommandText = "INSERT INTO test_ledger VALUES (42)";
                await command.ExecuteNonQueryAsync(cancellationToken);
                if (Interlocked.Increment(ref state.Attempts) == 1)
                    ((InterBus.Consuming.ConsumeContext<OrderPlaced>)context).Result = InterBus.Dispatching.DispatchResult.Release(new IOException("retry"));
            }
            await Task.Yield();
            if (!ReferenceEquals(transaction, context.GetPostgreSqlTransaction()))
                throw new InvalidOperationException("Transaction changed within a delivery");
            state.Seen.Add((transactionId, value));
        }
    }
}
