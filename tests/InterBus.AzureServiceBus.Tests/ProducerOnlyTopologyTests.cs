using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using InterBus.AzureServiceBus.Extensions;
using InterBus.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Testcontainers.ServiceBus;

namespace InterBus.AzureServiceBus.Tests;

public class ProducerOnlyTopologyTests
{
    private readonly ServiceBusContainer _container =
        new ServiceBusBuilder("mcr.microsoft.com/azure-messaging/servicebus-emulator:latest")
            .WithAcceptLicenseAgreement(true)
            .Build();

    private ServiceProvider _provider = null!;
    private CancellationTokenSource _cts = null!;
    private string _topicPrefix = null!;
    private string _queuePrefix = null!;

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
        var suffix = Guid.NewGuid().ToString("N")[..8];
        _topicPrefix = $"producer-only-topic-{suffix}-";
        _queuePrefix = $"producer-only-queue-{suffix}-";

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus =>
        {
            bus.AddAzureServiceBusTransport(o =>
            {
                o.ConnectionString = _container.GetConnectionString();
                o.AdminConnectionString = _container.GetHttpConnectionString();
                o.TopicPrefix = _topicPrefix;
                o.QueuePrefix = _queuePrefix;
            });
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
    public async Task Publish_NoConsumers_TopicIsEnsured()
    {
        using var scope = _provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

        var msg = new OrderPlaced(Guid.NewGuid(), "publish-only@test.com", 42m);
        await publisher.PublishAsync(msg, TestContext.CurrentContext.CancellationToken);

        var expectedTopicName = _topicPrefix + RoutingConventions.TopicName(typeof(OrderPlaced));
        var adminClient = new ServiceBusAdministrationClient(_container.GetHttpConnectionString());

        var exists = await adminClient.TopicExistsAsync(
            expectedTopicName,
            TestContext.CurrentContext.CancellationToken);

        Assert.That(exists.Value, Is.True,
            $"Expected topic '{expectedTopicName}' to exist after publish");
    }

    [Test]
    public async Task Send_NoConsumers_DoesNotEnsureQueue()
    {
        using var scope = _provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

        const string rawQueueName = "direct-no-consumer";
        var expectedQueueName = _queuePrefix + rawQueueName;

        var msg = new OrderPlaced(Guid.NewGuid(), "queue-only@test.com", 7m);
        Assert.ThrowsAsync<ServiceBusException>(async () =>
            await publisher.SendAsync(rawQueueName, msg, TestContext.CurrentContext.CancellationToken));

        var adminClient = new ServiceBusAdministrationClient(_container.GetHttpConnectionString());
        var exists = await adminClient.QueueExistsAsync(
            expectedQueueName,
            TestContext.CurrentContext.CancellationToken);

        Assert.That(exists.Value, Is.False,
            $"Expected queue '{expectedQueueName}' not to be created by send");
    }
}
