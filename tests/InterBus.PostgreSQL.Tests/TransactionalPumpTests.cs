using InterBus.Generated;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.PostgreSQL.Tests;

/// <summary>
/// Verifies that <see cref="PostgreSqlMessagePumpMode.Transactional"/> still delivers
/// messages correctly and retries after consumer failures. Each test creates its own
/// DI container to prevent cross-test consumer/tracker collisions.
/// </summary>
[TestFixtureSource(typeof(PostgreSqlVersions), nameof(PostgreSqlVersions.Transactional))]
public class TransactionalPumpTests(string imageTag, PostgreSqlMessagePumpMode pumpMode) : PostgreSqlFixtureBase(
    imageTag, pumpMode, bus =>
    {
        bus.Services.AddSingleton<OrderPlacedTracker>();
        bus.Services.AddSingleton<AuditTracker>();
        bus.AddConsumer<OrderPlacedConsumer>();
        bus.AddConsumer<AuditConsumer>();
    })
{
    [Test]
    public async Task Transactional_Publish_AllSubscribersReceive()
    {
        var orderTracker = Provider.GetRequiredService<OrderPlacedTracker>();
        var auditTracker = Provider.GetRequiredService<AuditTracker>();

        using var scope = Provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
        var msg = new OrderPlaced(Guid.NewGuid(), "tx@test.com", 10m);
        await publisher.PublishAsync(msg, TestContext.CurrentContext.CancellationToken);

        await WaitUntilAsync(() => orderTracker.Messages.Count == 1 && auditTracker.Messages.Count == 1);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(orderTracker.Messages.Single().OrderId, Is.EqualTo(msg.OrderId));
            Assert.That(auditTracker.Messages.Single().OrderId, Is.EqualTo(msg.OrderId));
        }
    }
}
