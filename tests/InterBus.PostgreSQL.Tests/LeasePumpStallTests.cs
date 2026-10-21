using InterBus.Generated;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.PostgreSQL.Tests;

[TestFixtureSource(typeof(PostgreSqlVersions), nameof(PostgreSqlVersions.Lease))]
public partial class LeasePumpStallTests(string imageTag, PostgreSqlMessagePumpMode pumpMode) : PostgreSqlFixtureBase(
    imageTag, pumpMode, bus =>
    {
        bus.Services.AddSingleton<OrderPlacedTracker>();
        bus.Services.AddSingleton<LeaseAtomicCounter>();
        bus.AddConsumer<LeaseStallingConsumer>();
    },
    postgreSql =>
    {
        postgreSql.LeaseDuration = TimeSpan.FromSeconds(3);
        postgreSql.LeaseRenewInterval = TimeSpan.FromSeconds(60);
        postgreSql.FallbackPollInterval = TimeSpan.FromMilliseconds(500);
        postgreSql.FailureRedeliveryDelay = TimeSpan.Zero;
    })
{
    [Test]
    public async Task Lease_StalledHandler_MessageIsReclaimedAfterExpiry()
    {
        var tracker = Provider.GetRequiredService<OrderPlacedTracker>();
        var counter = Provider.GetRequiredService<LeaseAtomicCounter>();

        using var scope = Provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
        var msg = new OrderPlaced(Guid.NewGuid(), "expire@test.com", 99m);
        await publisher.PublishAsync(msg, TestContext.CurrentContext.CancellationToken);

        await WaitUntilAsync(
            () => tracker.Messages.Count == 1,
            timeoutMs: 30_000);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(tracker.Messages.Single().OrderId, Is.EqualTo(msg.OrderId));
            Assert.That(counter.Value, Is.GreaterThanOrEqualTo(2),
                "Should have been delivered at least twice: once while stalling and once after lease expiry");
        }
    }
}
