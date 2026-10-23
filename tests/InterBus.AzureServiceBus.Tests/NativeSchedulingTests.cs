using InterBus.AzureServiceBus.Extensions;
using InterBus.Extensions;
using InterBus.Generated;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.AzureServiceBus.Tests;

[TestFixture(false)]
[TestFixture(true)]
public class NativeSchedulingTests(bool viaRequest) : ServiceBusFixture(bus =>
{
    bus.Services.AddSingleton<OrderPlacedTracker>();
    bus.Services.AddSingleton<AuditTracker>();
    bus.AddConsumer<OrderPlacedConsumer>();
    bus.AddConsumer<AuditConsumer>();
    bus.AddScheduling(s =>
    {
        s.AddAzureServiceBusScheduler();
        if (viaRequest) s.ViaRequest().AddSchedulingConsumer();
        else s.Directly();
    });
})
{
    [TestCase(false, false)]
    [TestCase(true, false)]
    [TestCase(false, true)]
    [TestCase(true, true)]
    public async Task ScheduledDeliveryHonorsTimeAndCancellation(bool topic, bool cancel)
    {
        Assume.That(UseRealAzure || topic || !cancel, Is.True,
            "Skipped: emulator queue cancellation bug Azure/azure-service-bus-emulator-installer#119. Run with INTERBUS_USE_REAL_AZURE=true.");
        using var scope = Provider.CreateScope();
        var scheduler = scope.ServiceProvider.GetRequiredService<IScheduler>();
        var tracker = Provider.GetRequiredService<OrderPlacedTracker>();
        var audit = Provider.GetRequiredService<AuditTracker>();
        var message = new OrderPlaced(Guid.NewGuid(), "scheduled@example.com", 10m);
        var due = DateTimeOffset.UtcNow.AddSeconds(cancel ? 15 : 5);
        var token = TestContext.CurrentContext.CancellationToken;
        var id = topic
            ? await scheduler.SchedulePublishAsync(message, due, token)
            : await scheduler.ScheduleSendAsync<OrderPlacedConsumer, OrderPlaced>(message, due, token);
        Assert.That(id, Is.Not.Null);
        if (cancel)
        {
            await scheduler.CancelAsync(id!, token);
            Assert.That(DateTimeOffset.UtcNow, Is.LessThan(due.AddSeconds(-5)),
                "Cancellation must finish well before activation to test broker cancellation");
        }
        while (DateTimeOffset.UtcNow < due.AddMilliseconds(-200))
        {
            Assert.That(tracker.Messages.Any(m => m.OrderId == message.OrderId), Is.False);
            Assert.That(audit.Messages.Any(m => m.OrderId == message.OrderId), Is.False);
            await Task.Delay(50, token);
        }
        if (cancel)
        {
            var end = due.AddSeconds(3);
            while (DateTimeOffset.UtcNow < end)
            {
                Assert.That(tracker.Messages.Any(m => m.OrderId == message.OrderId), Is.False);
                Assert.That(audit.Messages.Any(m => m.OrderId == message.OrderId), Is.False);
                await Task.Delay(50, token);
            }
        }
        else
        {
            await WaitUntilAsync(() => tracker.Messages.Any(m => m.OrderId == message.OrderId), timeoutMs: 15_000);
            if (topic) await WaitUntilAsync(() => audit.Messages.Any(m => m.OrderId == message.OrderId), timeoutMs: 15_000);
            else Assert.That(audit.Messages.Any(m => m.OrderId == message.OrderId), Is.False);
        }
    }
}
