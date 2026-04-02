using InterBus.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace InterBus.Tests;

/// <summary>
/// Unit tests for <see cref="TransportDelaySchedulingProvider"/> and the
/// <see cref="InterBusBuilderExtensions.AddTransportDelayScheduler"/> registration.
/// </summary>
public sealed class TransportDelaySchedulingTests
{
    #region Helpers

    private static (IMessageScheduler Scheduler, CapturingTransport Transport) BuildScheduler(
        Action<IInterBusBuilder>? configureBus = null)
    {
        var transport = new CapturingTransport();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus =>
        {
            bus.Services.AddSingleton<IInterBusTransport>(transport);
            bus.AddTransportDelayScheduler();
            bus.AddScheduling();
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
        var schedulingProvider = provider.GetService<IMessageSchedulingProvider>();

        Assert.NotNull(schedulingProvider);
    }

    [Fact]
    public void AddScheduling_AfterAddTransportDelayScheduler_RegistersIMessageScheduler()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus =>
        {
            bus.Services.AddSingleton<IInterBusTransport, NoOpTransport>();
            bus.AddTransportDelayScheduler();
            bus.AddScheduling();
        });

        var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var scheduler = scope.ServiceProvider.GetService<IMessageScheduler>();

        Assert.NotNull(scheduler);
    }

    #endregion

    #region SchedulePublish - delay passed to transport

    [Fact]
    public async Task SchedulePublish_PassesComputedDelayToTransport()
    {
        var (scheduler, transport) = BuildScheduler();
        var delay = TimeSpan.FromMinutes(5);
        var before = DateTimeOffset.UtcNow;

        await scheduler.SchedulePublish(new TestMessage(Guid.NewGuid()), before + delay,
            TestContext.Current.CancellationToken);

        var call = Assert.Single(transport.Sends);
        // The delay passed to the transport should be approximately equal to the requested delay.
        Assert.NotNull(call.Delay);
        Assert.True(call.Delay.Value >= TimeSpan.Zero);
        Assert.True(call.Delay.Value <= delay + TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task SchedulePublish_WithPastScheduledAt_ClampsDelayToZero()
    {
        var (scheduler, transport) = BuildScheduler();
        var pastTime = DateTimeOffset.UtcNow.AddMinutes(-10);

        await scheduler.SchedulePublish(new TestMessage(Guid.NewGuid()), pastTime,
            TestContext.Current.CancellationToken);

        var call = Assert.Single(transport.Sends);
        Assert.NotNull(call.Delay);
        Assert.Equal(TimeSpan.Zero, call.Delay.Value);
    }

    [Fact]
    public async Task SchedulePublish_SendsToTopicDestination()
    {
        var (scheduler, transport) = BuildScheduler();

        await scheduler.SchedulePublish(new TestMessage(Guid.NewGuid()),
            DateTimeOffset.UtcNow.AddMinutes(1), TestContext.Current.CancellationToken);

        var call = Assert.Single(transport.Sends);
        // Topic URI scheme ends with "topic"
        Assert.EndsWith("topic", call.Destination.Scheme, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task SchedulePublish_StampsMessageTypeHeader()
    {
        var (scheduler, transport) = BuildScheduler();

        await scheduler.SchedulePublish(new TestMessage(Guid.NewGuid()),
            DateTimeOffset.UtcNow.AddSeconds(10), TestContext.Current.CancellationToken);

        var call = Assert.Single(transport.Sends);
        Assert.True(call.Headers.ContainsKey(InterBusHeaders.MessageType));
        Assert.Contains(nameof(TestMessage), call.Headers[InterBusHeaders.MessageType]);
    }

    #endregion

    #region ScheduleSend - delay passed to transport

    [Fact]
    public async Task ScheduleSend_ByConsumerType_PassesDelayAndSendsToQueue()
    {
        var (scheduler, transport) = BuildScheduler();
        var scheduledAt = DateTimeOffset.UtcNow.AddSeconds(30);

        await scheduler.ScheduleSend<TestConsumer, TestMessage>(
            new TestMessage(Guid.NewGuid()), scheduledAt, TestContext.Current.CancellationToken);

        var call = Assert.Single(transport.Sends);
        // Queue URI scheme ends with "queue"
        Assert.EndsWith("queue", call.Destination.Scheme, StringComparison.OrdinalIgnoreCase);
        Assert.NotNull(call.Delay);
        Assert.True(call.Delay.Value >= TimeSpan.Zero);
    }

    [Fact]
    public async Task ScheduleSend_ByQueueName_PassesDelayAndSendsToNamedQueue()
    {
        var (scheduler, transport) = BuildScheduler();
        var scheduledAt = DateTimeOffset.UtcNow.AddHours(1);

        await scheduler.ScheduleSend("my-queue", new TestMessage(Guid.NewGuid()), scheduledAt,
            TestContext.Current.CancellationToken);

        var call = Assert.Single(transport.Sends);
        Assert.Equal("my-queue", RoutingConventions.GetDestinationName(call.Destination));
        Assert.NotNull(call.Delay);
        Assert.True(call.Delay.Value >= TimeSpan.Zero);
    }

    [Fact]
    public async Task ScheduleSend_WithPastScheduledAt_ClampsDelayToZero()
    {
        var (scheduler, transport) = BuildScheduler();

        await scheduler.ScheduleSend<TestConsumer, TestMessage>(
            new TestMessage(Guid.NewGuid()), DateTimeOffset.UtcNow.AddMinutes(-5),
            TestContext.Current.CancellationToken);

        var call = Assert.Single(transport.Sends);
        Assert.NotNull(call.Delay);
        Assert.Equal(TimeSpan.Zero, call.Delay.Value);
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

    private sealed class CapturingTransport : IInterBusTransport
    {
        public List<(Uri Destination, byte[] Payload, MessageHeaders Headers, TimeSpan? Delay)> Sends { get; } = [];

        public Task SendAsync(TransportSendContext context, CancellationToken cancellationToken)
        {
            Sends.Add((context.Destination, context.Payload, context.Headers, context.Delay));
            return Task.CompletedTask;
        }
    }

    #endregion
}
