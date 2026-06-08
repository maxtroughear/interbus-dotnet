using System.Collections.Concurrent;
using InterBus.Extensions;
using InterBus.Generated;
using InterBus.InMemory.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace InterBus.Tests.InMemory;

public class HeadersTests
{
    private static async Task<(ServiceProvider Provider, CancellationTokenSource Cts)> StartAsync(
        Action<IServiceCollection> registerSingletons,
        Action<InterBusBuilder> configureBus)
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
        await cts.CancelAsync();
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

    #region Helpers

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

    #endregion

    [Fact]
    public async Task Publish_WithHeaders_ConsumerReceivesHeaders()
    {
        var headerCapture = new HeaderCapture();

        var (provider, cts) = await StartAsync(
            services => { services.AddSingleton(headerCapture); },
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddConsumer<HeaderAwareConsumer>();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            var msg = new OrderPlaced(Guid.NewGuid(), "headers@test.com", 1.00m);

            await publisher.PublishAsync(msg, ctx =>
            {
                ctx.Headers["x-correlation-id"] = "abc123";
                ctx.Headers["x-tenant"] = "acme";
            }, TestContext.Current.CancellationToken);

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
            services => { services.AddSingleton(headerCapture); },
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddConsumer<HeaderAwareConsumer>();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            var msg = new OrderPlaced(Guid.NewGuid(), "send@test.com", 2.00m);

            await publisher.SendAsync<HeaderAwareConsumer, OrderPlaced>(msg,
                ctx => { ctx.Headers["x-source"] = "unit-test"; }, TestContext.Current.CancellationToken);

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
            services => { services.AddSingleton(correlationIds); },
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddConsumer<CorrelationIdCapturingConsumer>();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            var msg = new OrderPlaced(Guid.NewGuid(), "corr@test.com", 5.00m);

            await publisher.PublishAsync(msg, ctx => { ctx.Headers[InterBusHeaders.CorrelationId] = "corr-999"; },
                TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => correlationIds.Count == 1);

            Assert.Equal("corr-999", correlationIds.Single());
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }
}
