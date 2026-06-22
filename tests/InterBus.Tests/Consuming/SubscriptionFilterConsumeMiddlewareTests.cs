using InterBus.Consuming;
using InterBus.Filters;
using InterBus.Tests.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace InterBus.Tests.Consuming;

public sealed class SubscriptionFilterConsumeMiddlewareTests
{
    [Fact]
    public async Task InvokeAsync_SkipsNonMatchingMessage()
    {
        var descriptor = CreateDescriptor();
        var middleware = new SubscriptionFilterConsumeMiddleware();
        var nextCalled = false;

        await middleware.InvokeAsync(
            new ConsumeContext<OrderPlaced>(
                descriptor,
                new OrderPlaced(Guid.NewGuid(), "skip@example.com", 5m),
                CreateHeaders()),
            (_, _) =>
            {
                nextCalled = true;
                return Task.CompletedTask;
            },
            TestContext.Current.CancellationToken);

        Assert.False(nextCalled);
    }

    [Fact]
    public async Task InvokeAsync_AllowsMatchingMessage()
    {
        var descriptor = CreateDescriptor();
        var middleware = new SubscriptionFilterConsumeMiddleware();
        var nextCalled = false;

        await middleware.InvokeAsync(
            new ConsumeContext<OrderPlaced>(
                descriptor,
                new OrderPlaced(Guid.NewGuid(), "match@example.com", 15m),
                CreateHeaders()),
            (_, _) =>
            {
                nextCalled = true;
                return Task.CompletedTask;
            },
            TestContext.Current.CancellationToken);

        Assert.True(nextCalled);
    }

    private static ConsumerDescriptor CreateDescriptor()
    {
        var descriptor = new ConsumerDescriptor(
            typeof(TestConsumer),
            RoutingConventions.QueueUri("queue", "test-queue"));

        var builder = new ConsumerBuilder<TestConsumer>(descriptor, new ServiceCollection());
        builder.Consumes<TestConsumer, OrderPlaced>(
            RoutingConventions.MessageTypeString(typeof(OrderPlaced)));
        builder.Filter<OrderPlaced>(order => order.CustomerEmail == "match@example.com");
        return descriptor;
    }

    private static MessageHeaders CreateHeaders() =>
        new()
        {
            [InterBusHeaders.MessageType] = RoutingConventions.MessageTypeString(typeof(OrderPlaced)),
        };

    internal sealed class TestConsumer : IConsumer<OrderPlaced>
    {
        public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;
    }
}
