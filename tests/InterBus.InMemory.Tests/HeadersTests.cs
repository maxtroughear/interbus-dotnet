using System.Collections.Concurrent;
using InterBus.Extensions;
using InterBus.Generated;
using InterBus.InMemory.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace InterBus.InMemory.Tests;

public class HeadersTests
{
    private static async Task<(ServiceProvider Provider, CancellationTokenSource Cts)> StartAsync(
        Action<IServiceCollection> registerSingletons,
        Action<IInterBusBuilder> configureBus)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        registerSingletons(services);
        services.AddInterBus(configureBus);

        var provider = services.BuildServiceProvider();
        var cts = new CancellationTokenSource();

        foreach (var svc in provider.GetServices<IHostedService>())
            await svc.StartAsync(cts.Token);

        return (provider, cts);
    }

    private static async Task StopAsync(ServiceProvider provider, CancellationTokenSource cts)
    {
        cts.Cancel();
        foreach (var svc in provider.GetServices<IHostedService>())
        {
            try
            {
                await svc.StopAsync(CancellationToken.None);
            }
            catch (OperationCanceledException)
            {
            }
        }

        await provider.DisposeAsync();
        cts.Dispose();
    }

    private static async Task WaitUntilAsync(Func<bool> condition, int timeoutMs = 5000, int intervalMs = 25)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (!condition() && DateTime.UtcNow < deadline)
            await Task.Delay(intervalMs);
    }

    // ---- helpers ----

    /// <summary>Captures headers seen by the consumer during Consume.</summary>
    public sealed class HeaderCapture
    {
        private readonly ConcurrentBag<IReadOnlyDictionary<string, string>> _captured = [];
        public IReadOnlyCollection<IReadOnlyDictionary<string, string>> Captured => _captured;
        public void Add(IReadOnlyDictionary<string, string> headers) => _captured.Add(headers);
    }

    public sealed class HeaderAwareConsumer(
        HeaderCapture headerCapture) : IConsumer<OrderPlaced>
    {
        public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            headerCapture.Add(context.Headers);
            return Task.CompletedTask;
        }
    }

    public sealed class CorrelationIdCapturingConsumer(
        ConcurrentBag<string?> correlationIds) : IConsumer<OrderPlaced>
    {
        public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            correlationIds.Add(context.CorrelationId);
            return Task.CompletedTask;
        }
    }

    // ---- tests ----

    [Fact]
    public async Task Publish_WithHeaders_ConsumerReceivesHeaders()
    {
        var headerCapture = new HeaderCapture();

        var (provider, cts) = await StartAsync(
            services =>
            {
                services.AddSingleton(headerCapture);
            },
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddConsumers();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
            var msg = new OrderPlaced(Guid.NewGuid(), "headers@test.com", 1.00m);
            var headers = new MessageHeaders { ["x-correlation-id"] = "abc123", ["x-tenant"] = "acme" };

            await publisher.Publish(msg, headers, TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => headerCapture.Captured.Count == 1);

            Assert.Single(headerCapture.Captured);
            var received = headerCapture.Captured.Single();
            Assert.Equal("abc123", received["x-correlation-id"]);
            Assert.Equal("acme", received["x-tenant"]);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task Send_WithHeaders_ConsumerReceivesHeaders()
    {
        var headerCapture = new HeaderCapture();

        var (provider, cts) = await StartAsync(
            services =>
            {
                services.AddSingleton(headerCapture);
            },
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddConsumers();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
            var msg = new OrderPlaced(Guid.NewGuid(), "send@test.com", 2.00m);
            var headers = new MessageHeaders { ["x-source"] = "unit-test" };

            await publisher.Send<HeaderAwareConsumer, OrderPlaced>(msg, headers, TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => headerCapture.Captured.Count == 1);

            Assert.Single(headerCapture.Captured);
            Assert.Equal("unit-test", headerCapture.Captured.Single()["x-source"]);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task Publish_WithoutUserHeaders_ConsumerReceivesOnlySystemHeaders()
    {
        var headerCapture = new HeaderCapture();

        var (provider, cts) = await StartAsync(
            services =>
            {
                services.AddSingleton(headerCapture);
            },
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddConsumers();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
            var msg = new OrderPlaced(Guid.NewGuid(), "noheaders@test.com", 3.00m);

            await publisher.Publish(msg, TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => headerCapture.Captured.Count == 1);

            Assert.Single(headerCapture.Captured);
            var received = headerCapture.Captured.Single();
            // The publisher always stamps the interbus-message-type header; no user headers present.
            Assert.Single(received);
            Assert.Equal(
                typeof(OrderPlaced).FullName,
                received[InterBusHeaders.MessageType]);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public void MessageHeaders_KeyLookup_IsCaseInsensitive()
    {
        var headers = new MessageHeaders { ["X-Correlation-Id"] = "test-id" };

        Assert.Equal("test-id", headers["x-correlation-id"]);
        Assert.Equal("test-id", headers["X-CORRELATION-ID"]);
    }

    [Fact]
    public async Task Publish_WithCorrelationIdHeader_ContextExposesConvenienceProperty()
    {
        var correlationIds = new ConcurrentBag<string?>();

        var (provider, cts) = await StartAsync(
            services =>
            {
                services.AddSingleton(correlationIds);
            },
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddConsumers();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
            var msg = new OrderPlaced(Guid.NewGuid(), "corr@test.com", 5.00m);
            var headers = new MessageHeaders { [InterBusHeaders.CorrelationId] = "corr-999" };

            await publisher.Publish(msg, headers, TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => correlationIds.Count == 1);

            Assert.Equal("corr-999", correlationIds.Single());
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }
}
