using InterBus.AzureServiceBus.Extensions;
using InterBus.Extensions;
using InterBus.Generated;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace InterBus.AzureServiceBus.Tests;

/// <summary>
/// Integration tests for Azure Service Bus request/reply using session-based virtual queues.
/// </summary>
[TestFixture]
public class RequestReplyTests : ServiceBusFixture
{
    private ServiceProvider _provider;
    private CancellationTokenSource _cts;

    [SetUp]
    public async Task Setup()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddInterBus(bus =>
        {
            bus.AddAzureServiceBusTransport(o =>
            {
                o.ConnectionString = Container.GetConnectionString();
                o.AdminConnectionString = Container.GetHttpConnectionString();
            });
            bus.AddConsumer<GetOrderStatusConsumer>();
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
    public async Task RequestAsync_ConsumerReplies_ReturnsReply()
    {
        using var scope = _provider.CreateScope();
        var client = scope.ServiceProvider
            .GetRequiredService<IRequestClient<GetOrderStatusRequest, GetOrderStatusReply>>();

        var orderId = Guid.NewGuid();
        var reply = await client.RequestAsync(
            new GetOrderStatusRequest(orderId),
            TestContext.CurrentContext.CancellationToken);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(reply.OrderId, Is.EqualTo(orderId));
            Assert.That(reply.Status, Is.EqualTo("Shipped"));
        }
    }
}
