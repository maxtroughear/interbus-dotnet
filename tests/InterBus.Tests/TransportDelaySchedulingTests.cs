using InterBus.Extensions;
using InterBus.Publishing;
using InterBus.Scheduling;
using InterBus.Transporting;
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

    private static (IScheduler Scheduler, RecordingTransport Transport) BuildScheduler(
        Action<InterBusBuilder>? configureBus = null)
    {
        var transport = new RecordingTransport();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus =>
        {
            bus.Services.AddKeyedSingleton<ITransport>("topic", transport);
            bus.Services.AddKeyedSingleton<ITransport>("queue", transport);
            bus.AddScheduling(builder => builder.OnTransport());
            configureBus?.Invoke(bus);
        });

        var provider = services.BuildServiceProvider();
        var scope = provider.CreateScope();
        var scheduler = scope.ServiceProvider.GetRequiredService<IScheduler>();
        return (scheduler, transport);
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
            bus.Services.AddKeyedSingleton<ITransport>("queue", transport);
            bus.AddScheduling(builder => builder.OnTransport());
        });

        var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var schedulingProvider = scope.ServiceProvider.GetRequiredService<IMessageScheduler>();

        var delay = TimeSpan.FromMinutes(5);
        var scheduledAt = DateTimeOffset.UtcNow + delay;

        await schedulingProvider.ScheduleInternalAsync(
            new PublishContext<string>(new Uri("queue://interbus/my-queue"), "payload", new MessageHeaders()),
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
            bus.Services.AddKeyedSingleton<ITransport>("queue", transport);
            bus.AddScheduling(builder => builder.OnTransport());
        });

        var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var schedulingProvider = scope.ServiceProvider.GetRequiredService<IMessageScheduler>();

        var scheduledAt = DateTimeOffset.UtcNow.AddMinutes(-10); // already in the past

        await schedulingProvider.ScheduleInternalAsync(
            new PublishContext<string>(new Uri("queue://interbus/my-queue"), "payload", new MessageHeaders()),
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

        await scheduler.SchedulePublishAsync(new TestMessage(), scheduledAt,
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
            new TestMessage(), scheduledAt, TestContext.Current.CancellationToken);

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

        await scheduler.ScheduleSendAsync("my-queue", new TestMessage(), scheduledAt,
            TestContext.Current.CancellationToken);

        var sent = Assert.Single(transport.Sent);
        Assert.Equal("my-queue", RoutingConventions.GetDestinationName(sent.Destination));
        Assert.NotNull(sent.Delay);
        Assert.True(sent.Delay > TimeSpan.Zero);
    }

    #endregion

    #region Support types

    private sealed record TestMessage;

    private sealed class TestConsumer : IConsumer<TestMessage>
    {
        public Task Consume(TestMessage message, IConsumeContext context,
            CancellationToken cancellationToken = default) => Task.CompletedTask;
    }

    private sealed class RecordingTransport : ITransport
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
