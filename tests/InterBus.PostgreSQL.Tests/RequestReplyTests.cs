using InterBus.Generated;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.PostgreSQL.Tests;

public class RequestReplyTests(string imageTag, PostgreSqlMessagePumpMode pumpMode)
    : PostgreSqlFixtureBase(imageTag, pumpMode,
        bus =>
        {
            bus.AddConsumer<GetOrderStatusConsumer>();
        })
{
    [Test]
    [CancelAfter(30_000)]
    public async Task RequestAsync_ConsumerReplies_ReturnsReply()
    {
        using var scope = Provider.CreateScope();
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
        using var scope = Provider.CreateScope();
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
