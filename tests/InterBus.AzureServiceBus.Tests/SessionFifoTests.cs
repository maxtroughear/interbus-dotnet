using InterBus.AzureServiceBus.Extensions;
using InterBus.Extensions;
using InterBus.Generated;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace InterBus.AzureServiceBus.Tests;

/// <summary>
/// Integration tests for Azure Service Bus session-based FIFO processing.
/// Each test provisions a fresh container and service provider to avoid
/// cross-test entity collisions.
/// </summary>
[TestFixture]
public class SessionFifoTests : ServiceBusFixture
{
    private ServiceProvider _provider;
    private CancellationTokenSource _cts;
    private SessionOrderPlacedTracker _tracker;

    [SetUp]
    public async Task Setup()
    {
        _tracker = new SessionOrderPlacedTracker();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(_tracker);

        services.AddInterBus(bus =>
        {
            bus.AddAzureServiceBusTransport(o =>
            {
                o.ConnectionString = _container.GetConnectionString();
                o.AdminConnectionString = _container.GetHttpConnectionString();
            });
            bus.Configure<SessionOrderPlaced>(c => c.Fifo(o => o.CustomerId));
            bus.AddConsumer<SessionOrderPlacedConsumer>(c => c.WithFifo());
        });

        _provider = services.BuildServiceProvider();
        _cts = new CancellationTokenSource();

        foreach (var svc in _provider.GetServices<IHostedService>())
            await svc.StartAsync(_cts.Token);
    }

    [TearDown]
    public async Task TearDown()
    {
        await _cts.CancelAsync();

        foreach (var svc in _provider.GetServices<IHostedService>())
            await svc.StopAsync(CancellationToken.None);

        _cts.Dispose();
        await _provider.DisposeAsync();
    }

    [Test]
    public async Task Publish_FifoConsumer_MessageIsDelivered()
    {
        using var scope = _provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

        var msg = new SessionOrderPlaced(Guid.NewGuid(), "cust-1", 99.99m);
        await publisher.PublishAsync(msg, TestContext.CurrentContext.CancellationToken);

        await WaitUntilAsync(() => _tracker.Messages.Count >= 1, timeoutMs: 30_000);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(_tracker.Messages, Has.Count.EqualTo(1));
            Assert.That(_tracker.Messages.Single().OrderId, Is.EqualTo(msg.OrderId));
        }
    }

    [Test]
    public async Task Publish_FifoConsumer_MultipleGroupsAllDelivered()
    {
        using var scope = _provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

        var messages = Enumerable.Range(1, 5)
            .Select(i => new SessionOrderPlaced(Guid.NewGuid(), $"cust-{i}", i * 10m))
            .ToList();

        foreach (var msg in messages)
            await publisher.PublishAsync(msg, TestContext.CurrentContext.CancellationToken);

        await WaitUntilAsync(() => _tracker.Messages.Count >= messages.Count, timeoutMs: 30_000);

        Assert.That(_tracker.Messages, Has.Count.EqualTo(messages.Count));
        Assert.That(
            _tracker.Messages.Select(m => m.OrderId),
            Is.EquivalentTo(messages.Select(m => m.OrderId)));
    }
}
