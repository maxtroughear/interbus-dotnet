using InterBus.Extensions;
using InterBus.Generated;
using InterBus.PostgreSQL.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Testcontainers.PostgreSql;

namespace InterBus.PostgreSQL.Tests;

[TestFixtureSource(typeof(PostgreSqlVersions), nameof(PostgreSqlVersions.All))]
public class RequestReplyTests(string imageTag)
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder(imageTag).Build();
    private ServiceProvider _provider = null!;
    private CancellationTokenSource _cts = null!;

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
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus =>
        {
            bus.AddPostgreSqlTransport(o =>
            {
                o.ConnectionString = _container.GetConnectionString();
                o.ConnectionRetry.RetryCount = 5;
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
    [CancelAfter(30_000)]
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

    [Test]
    [CancelAfter(30_000)]
    public async Task RequestAsync_MultipleConcurrentRequests_AllRepliesRouted()
    {
        using var scope = _provider.CreateScope();
        var client = scope.ServiceProvider
            .GetRequiredService<IRequestClient<GetOrderStatusRequest, GetOrderStatusReply>>();

        var orderIds = Enumerable.Range(0, 5).Select(_ => Guid.NewGuid()).ToList();
        var ct = TestContext.CurrentContext.CancellationToken;

        var replies = await Task.WhenAll(
            orderIds.Select(id => client.RequestAsync(new GetOrderStatusRequest(id), ct)));

        Assert.That(replies.Select(r => r.OrderId), Is.EquivalentTo(orderIds));
        Assert.That(replies, Has.All.Matches<GetOrderStatusReply>(r => r.Status == "Shipped"));
    }
}
