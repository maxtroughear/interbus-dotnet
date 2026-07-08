using InterBus.Generated;
using InterBus.Tests.Common;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.AmazonSqs.Tests;

[TestFixture]
public class RequestReplyTests() : LocalstackTestFixtureBase(bus => { bus.AddConsumer<GetOrderStatusConsumer>(); })
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
}
