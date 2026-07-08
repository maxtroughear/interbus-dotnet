using InterBus.Generated;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.PostgreSQL.Tests;

/// <summary>
/// Lease-pump specific integration tests.
/// Uses short lease durations to exercise renewal, release-on-failure, and expired-lease reclaim
/// within reasonable time bounds.
/// </summary>
[TestFixtureSource(typeof(PostgreSqlVersions), nameof(PostgreSqlVersions.Lease))]
public class LeasePumpTests(string imageTag, PostgreSqlMessagePumpMode pumpMode) : PostgreSqlFixtureBase(
    imageTag, pumpMode, bus =>
    {
        bus.Services.AddSingleton<OrderPlacedTracker>();
        bus.AddConsumer<OrderPlacedConsumer>();
    })
{
    [Test]
    public async Task Lease_Publish_MessageIsDelivered()
    {
        var tracker = Provider.GetRequiredService<OrderPlacedTracker>();

        using var scope = Provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
        var msg = new OrderPlaced(Guid.NewGuid(), "lease@test.com", 50m);
        await publisher.PublishAsync(msg, TestContext.CurrentContext.CancellationToken);

        await WaitUntilAsync(() => tracker.Messages.Count == 1);
        Assert.That(tracker.Messages.Single().OrderId, Is.EqualTo(msg.OrderId));
    }
}
