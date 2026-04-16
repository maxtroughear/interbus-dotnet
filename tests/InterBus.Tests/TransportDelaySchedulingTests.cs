using InterBus.Extensions;
using InterBus.Middleware;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace InterBus.Tests;

/// <summary>
/// Unit tests for <c>TransportDelaySchedulingProvider</c> and its registration via
/// <c>AddTransportDelayScheduler()</c>.
/// </summary>
public sealed class TransportDelaySchedulingTests
{
    #region Helpers

    private static (IMessageScheduler Scheduler, RecordingTransport Transport) BuildScheduler(
        Action<IInterBusBuilder>? configureBus = null)
    {
        var transport = new RecordingTransport();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus =>
        {
            bus.Services.AddKeyedSingleton<IInterBusTransport>("topic", transport);
            bus.Services.AddKeyedSingleton<IInterBusTransport>("queue", transport);
            bus.AddScheduling();
            bus.AddTransportDelayScheduler();
            configureBus?.Invoke(bus);
        });

        var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var scheduler = scope.ServiceProvider.GetRequiredService<IMessageScheduler>();
        return (scheduler, transport);
    }

    #endregion

    #region Registration

    [Fact]
    public void AddTransportDelayScheduler_RegistersIMessageSchedulingProvider()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus =>
        {
            bus.Services.AddSingleton<IInterBusTransport, NoOpTransport>();
            bus.AddTransportDelayScheduler();
        });

        var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var schedulingProvider = scope.ServiceProvider.GetService<IMessageSchedulingProvider>();

        Assert.NotNull(schedulingProvider);
    }

    #endregion

    #region Delay computation

    [Fact]
    public async Task ScheduleAsync_FutureScheduledAt_PassesPositiveDelayToTransport()
    {
        var transport = new RecordingTransport();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus =>
        {
            bus.Services.AddKeyedSingleton<IInterBusTransport>("queue", transport);
            bus.AddTransportDelayScheduler();
        });

        var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var schedulingProvider = scope.ServiceProvider.GetRequiredService<IMessageSchedulingProvider>();

        var delay = TimeSpan.FromMinutes(5);
        var scheduledAt = DateTimeOffset.UtcNow + delay;

        await schedulingProvider.ScheduleAsync(
            new Uri("queue://interbus/my-queue"),
            [1, 2, 3],
            new MessageHeaders(),
            scheduledAt,
            TestContext.Current.CancellationToken);

        var sent = Assert.Single(transport.Sent);
        Assert.NotNull(sent.Delay);
        Assert.True(sent.Delay > TimeSpan.Zero);
        Assert.True(sent.Delay <= delay);
    }

    [Fact]
    public async Task ScheduleAsync_PastScheduledAt_PassesZeroDelayToTransport()
    {
        var transport = new RecordingTransport();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus =>
        {
            bus.Services.AddKeyedSingleton<IInterBusTransport>("queue", transport);
            bus.AddTransportDelayScheduler();
        });

        var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var schedulingProvider = scope.ServiceProvider.GetRequiredService<IMessageSchedulingProvider>();

        var scheduledAt = DateTimeOffset.UtcNow.AddMinutes(-10); // already in the past

        await schedulingProvider.ScheduleAsync(
            new Uri("queue://interbus/my-queue"),
            [1, 2, 3],
            new MessageHeaders(),
            scheduledAt,
            TestContext.Current.CancellationToken);

        var sent = Assert.Single(transport.Sent);
        Assert.Equal(TimeSpan.Zero, sent.Delay);
    }

    #endregion

    #region IMessageScheduler integration

    [Fact]
    public async Task SchedulePublish_ViaTransportDelayScheduler_SendsToTransportWithDelay()
    {
        var (scheduler, transport) = BuildScheduler();
        var scheduledAt = DateTimeOffset.UtcNow.AddMinutes(10);

        await scheduler.SchedulePublishAsync(new TestMessage(Guid.NewGuid()), scheduledAt,
            TestContext.Current.CancellationToken);

        var sent = Assert.Single(transport.Sent);
        Assert.Equal("topic", sent.Destination.Scheme);
        Assert.NotNull(sent.Delay);
        Assert.True(sent.Delay > TimeSpan.Zero);
    }

    [Fact]
    public async Task ScheduleSend_ByConsumerType_ViaTransportDelayScheduler_SendsToTransportWithDelay()
    {
        var (scheduler, transport) = BuildScheduler();
        var scheduledAt = DateTimeOffset.UtcNow.AddSeconds(30);

        await scheduler.ScheduleSendAsync<TestConsumer, TestMessage>(
            new TestMessage(Guid.NewGuid()), scheduledAt, TestContext.Current.CancellationToken);

        var sent = Assert.Single(transport.Sent);
        Assert.Equal("queue", sent.Destination.Scheme);
        Assert.NotNull(sent.Delay);
        Assert.True(sent.Delay > TimeSpan.Zero);
    }

    [Fact]
    public async Task ScheduleSend_ByQueueName_ViaTransportDelayScheduler_SendsToTransportWithDelay()
    {
        var (scheduler, transport) = BuildScheduler();
        var scheduledAt = DateTimeOffset.UtcNow.AddHours(1);

        await scheduler.ScheduleSendAsync("my-queue", new TestMessage(Guid.NewGuid()), scheduledAt,
            TestContext.Current.CancellationToken);

        var sent = Assert.Single(transport.Sent);
        Assert.Equal("my-queue", RoutingConventions.GetDestinationName(sent.Destination));
        Assert.NotNull(sent.Delay);
        Assert.True(sent.Delay > TimeSpan.Zero);
    }

    #endregion

    #region ScheduleContext.Delay forwarded through middleware pipeline

    [Fact]
    public async Task SchedulePipelineExecutor_ForwardsDelay_ToTransportSendContext()
    {
        var transport = new RecordingTransport();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus => { bus.Services.AddKeyedSingleton<IInterBusTransport>("queue", transport); });

        var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var executor = scope.ServiceProvider.GetRequiredService<SchedulePipelineExecutor>();
        var expectedDelay = TimeSpan.FromMinutes(3);
        var ctx = new ScheduleContext(new Uri("queue://interbus/my-queue"), [1], new MessageHeaders(), expectedDelay);

        await executor.ExecuteAsync(ctx, TestContext.Current.CancellationToken);

        var sent = Assert.Single(transport.Sent);
        Assert.Equal(expectedDelay, sent.Delay);
    }

    [Fact]
    public async Task SchedulePipelineExecutor_NullDelay_PassesNullToTransport()
    {
        var transport = new RecordingTransport();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus => { bus.Services.AddKeyedSingleton<IInterBusTransport>("queue", transport); });

        var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var executor = scope.ServiceProvider.GetRequiredService<SchedulePipelineExecutor>();
        var ctx = new ScheduleContext(new Uri("queue://interbus/my-queue"), [1], new MessageHeaders(), null);

        await executor.ExecuteAsync(ctx, TestContext.Current.CancellationToken);

        var sent = Assert.Single(transport.Sent);
        Assert.Null(sent.Delay);
    }

    #endregion

    #region Support types

    private sealed record TestMessage(Guid Id);

    private sealed class TestConsumer : IConsumer<TestMessage>
    {
        public Task Consume(TestMessage message, IConsumeContext context,
            CancellationToken cancellationToken = default) => Task.CompletedTask;
    }

    private sealed class NoOpTransport : IInterBusTransport
    {
        public Task SendAsync(TransportSendContext context, CancellationToken cancellationToken) =>
            Task.CompletedTask;
    }

    private sealed class RecordingTransport : IInterBusTransport
    {
        public List<TransportSendContext> Sent { get; } = [];

        public Task SendAsync(TransportSendContext context, CancellationToken cancellationToken)
        {
            Sent.Add(context);
            return Task.CompletedTask;
        }
    }

    #endregion
}
