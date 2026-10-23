using InterBus.AzureServiceBus.Extensions;
using InterBus.Extensions;
using InterBus.Generated;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.AzureServiceBus.Tests;

public class NativeSessionSchedulingTests() : ServiceBusFixture(bus =>
{
    bus.Services.AddSingleton<SessionOrderPlacedTracker>();
    bus.Configure<SessionOrderPlaced>(c => c.Fifo(o => o.CustomerId));
    bus.AddConsumer<SessionOrderPlacedConsumer>(c => c.WithFifo());
    bus.AddScheduling(s => s.AddAzureServiceBusScheduler().Directly());
})
{
    [Test]
    public async Task ScheduledSessionMessageIsDelivered()
    {
        using var scope = Provider.CreateScope();
        var scheduler = scope.ServiceProvider.GetRequiredService<IScheduler>();
        var message = new SessionOrderPlaced(Guid.NewGuid(), "scheduled-session", 10m);
        await scheduler.SchedulePublishAsync(message, DateTimeOffset.UtcNow.AddSeconds(2), TestContext.CurrentContext.CancellationToken);
        var tracker = Provider.GetRequiredService<SessionOrderPlacedTracker>();
        await WaitUntilAsync(() => tracker.Messages.Any(m => m.OrderId == message.OrderId), timeoutMs: 15_000);
    }
}
