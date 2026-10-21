using InterBus.Generated;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.AzureServiceBus.Tests;

[TestFixture]
public partial class SessionConcurrencyTests() : ServiceBusFixture(bus =>
{
    bus.Services.AddSingleton<Gate>();
    bus.Configure<SessionOrderPlaced>(options => options.Fifo(message => message.CustomerId));
    bus.AddConsumer<BlockingSessionConsumer>(consumer => consumer.WithFifo().WithConcurrency(2));
})
{
    [Test]
    public async Task IndependentSessions_Overlap_WhileEachSessionRemainsOrdered()
    {
        var gate = Provider.GetRequiredService<Gate>();
        try
        {
            await using var scope = Provider.CreateAsyncScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            foreach (var group in new[] { "a", "b" })
                for (var sequence = 0; sequence < 2; sequence++)
                    await publisher.PublishAsync(new SessionOrderPlaced(Guid.NewGuid(), group, sequence));
            await gate.BothEntered.Task.WaitAsync(TimeSpan.FromSeconds(30));
            Assert.That(gate.Active, Is.EqualTo(2));
            Assert.That(gate.Started, Has.Count.EqualTo(2));
            Assert.That(gate.Started.All(item => item.Sequence == 0), Is.True);
        }
        finally { gate.Release.TrySetResult(); }
        await gate.Completed.Task.WaitAsync(TimeSpan.FromSeconds(30));
        foreach (var group in new[] { "a", "b" })
            Assert.That(gate.Started.Where(item => item.Group == group).Select(item => item.Sequence), Is.EqualTo(new[] { 0, 1 }));
        Assert.That(gate.Maximum, Is.EqualTo(2));
        Assert.That(gate.PerSessionMaximum.Values, Is.All.EqualTo(1));
    }
}
