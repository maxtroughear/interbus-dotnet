using InterBus.Generated;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.AzureServiceBus.Tests;

/// <summary>
/// Integration tests for Azure Service Bus request/reply using session-based virtual queues.
/// </summary>
[TestFixture]
public class RequestReplyTests() : ServiceBusFixture(bus => { bus.AddConsumer<GetOrderStatusConsumer>(); })
{
    [Test]
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
