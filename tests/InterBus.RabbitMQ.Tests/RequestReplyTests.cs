using InterBus.Extensions;
using InterBus.Generated;
using InterBus.RabbitMQ.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Testcontainers.RabbitMq;

namespace InterBus.RabbitMQ.Tests;

public record GetOrderStatusRequest(Guid OrderId);

public record GetOrderStatusReply(Guid OrderId, string Status);

/// <summary>
/// Consumer that handles <see cref="GetOrderStatusRequest"/> and sends a reply back
/// to the temporary reply queue using the <see cref="IConsumeContext.ReplyTo"/> address.
/// </summary>
public sealed class GetOrderStatusConsumer(IPublisher publisher) : IConsumer<GetOrderStatusRequest>
{
    public async Task Consume(GetOrderStatusRequest message, IConsumeContext context,
        CancellationToken cancellationToken = default)
    {
        if (context.ReplyTo is null) return;
        await publisher.SendAsync(
            context.ReplyTo,
            new GetOrderStatusReply(message.OrderId, "Shipped"),
            cancellationToken);
    }
}

/// <summary>
/// Integration tests for RabbitMQ request/reply using virtual queues.
/// </summary>
[TestFixtureSource(typeof(RabbitMqVersions), nameof(RabbitMqVersions.All))]
public class RequestReplyTests(string imageTag)
{
    private readonly RabbitMqContainer _container = new RabbitMqBuilder(imageTag).Build();
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
            bus.AddRabbitMqTransport(o => { o.Uri = _container.GetConnectionString(); });
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
}
