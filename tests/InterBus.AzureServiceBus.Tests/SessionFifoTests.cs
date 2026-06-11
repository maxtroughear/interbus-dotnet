using InterBus.AzureServiceBus.Extensions;
using InterBus.Extensions;
using InterBus.Generated;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Testcontainers.ServiceBus;

namespace InterBus.AzureServiceBus.Tests;

/// <summary>
/// Integration tests for Azure Service Bus session-based FIFO processing.
/// Each test provisions a fresh container and service provider to avoid
/// cross-test entity collisions.
/// </summary>
[TestFixture]
public class SessionFifoTests
{
    private readonly ServiceBusContainer _container =
        new ServiceBusBuilder("mcr.microsoft.com/azure-messaging/servicebus-emulator:latest")
            .WithAcceptLicenseAgreement(true)
            .Build();

    private ServiceProvider _provider = null!;
    private CancellationTokenSource _cts = null!;
    private SessionOrderPlacedTracker _tracker = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        await _container.StartAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _container.StopAsync();
        await _container.DisposeAsync();
    }

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

        await TestHelpers.WaitUntilAsync(() => _tracker.Messages.Count >= 1);

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

        await TestHelpers.WaitUntilAsync(() => _tracker.Messages.Count >= messages.Count);

        Assert.That(_tracker.Messages, Has.Count.EqualTo(messages.Count));
        Assert.That(
            _tracker.Messages.Select(m => m.OrderId),
            Is.EquivalentTo(messages.Select(m => m.OrderId)));
    }
}
