using InterBus.Generated;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace InterBus.PostgreSQL.Tests;

[TestFixtureSource(typeof(PostgreSqlVersions), nameof(PostgreSqlVersions.Lease))]
public class LeasePrefetchTests(string imageTag, PostgreSqlMessagePumpMode mode) : PostgreSqlFixtureBase(
    imageTag, mode, bus =>
    {
        bus.Services.AddSingleton<LeaseConcurrencyBoundTests.Gate>();
        bus.AddConsumer<LeaseConcurrencyBoundTests.BlockingConsumer>(consumer => consumer.WithConcurrency(2).WithPumpCount(4));
    }, options => options.PrefetchCount = 3)
{
    [Test]
    public async Task LeasePrefetch_IsSharedAcrossPumpsAndProcessing()
    {
        var gate = Provider.GetRequiredService<LeaseConcurrencyBoundTests.Gate>();
        try
        {
            await using var scope = Provider.CreateAsyncScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            for (var i = 0; i < 8; i++)
                await publisher.PublishAsync(new OrderPlaced(Guid.NewGuid(), "prefetch", i));
            await gate.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            var dataSource = Provider.GetRequiredService<NpgsqlDataSource>();
            await WaitUntilAsync(async () =>
            {
                await using var query = dataSource.CreateCommand("SELECT COUNT(*) FROM interbus.messages WHERE lease_token IS NOT NULL");
                var owned = (long)(await query.ExecuteScalarAsync())!;
                Assert.That(owned, Is.LessThanOrEqualTo(5));
                return owned == 5;
            }, cancellationToken: TestContext.CurrentContext.CancellationToken);
            Assert.That(gate.Active, Is.EqualTo(2));
            Assert.That(gate.Maximum, Is.EqualTo(2));
        }
        finally { gate.Release.TrySetResult(); }
        await gate.Completed.Task.WaitAsync(TimeSpan.FromSeconds(10));
    }
}
