using InterBus.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace InterBus.Tests;

/// <summary>
/// Unit tests for <see cref="PublishMessageScheduler"/> - the publish-based
/// <see cref="IMessageScheduler"/> implementation registered via
/// <see cref="InterBusBuilderExtensions.AddPublishScheduling"/>.
/// </summary>
public sealed class PublishSchedulingTests
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
            bus.Services.AddKeyedSingleton<IInterBusTransport>("topic", transport);
            bus.Services.AddKeyedSingleton<IInterBusTransport>("queue", transport);
            bus.AddPublishScheduling();
            bus.AddSchedulingConsumer();
            configureBus?.Invoke(bus);
        });

        var provider = services.BuildServiceProvider();
        var scope = provider.CreateScope();
        var scheduler = scope.ServiceProvider.GetRequiredService<IMessageScheduler>();
        return (scheduler, transport);
    }

    #endregion

    #region AddPublishScheduling - registration

    [Fact]
    public void AddPublishScheduling_RegistersIMessageScheduler()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus =>
        {
            bus.Services.AddSingleton<IInterBusTransport, NoOpTransport>();
            bus.AddPublishScheduling();
        });

        var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var scheduler = scope.ServiceProvider.GetService<IMessageScheduler>();

        Assert.NotNull(scheduler);
    }

    [Fact]
    public void AddPublishScheduling_NotRegistered_WhenNotConfigured()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus => { bus.Services.AddSingleton<IInterBusTransport, NoOpTransport>(); });

        var provider = services.BuildServiceProvider();
        var scheduler = provider.GetService<IMessageScheduler>();

        Assert.Null(scheduler);
    }

    #endregion

    #region SchedulePublish - forwards command to scheduling consumer queue

    [Fact]
    public async Task SchedulePublish_SendsCommandToSchedulingConsumerQueue()
    {
        var (scheduler, transport) = BuildScheduler();
        var scheduledAt = DateTimeOffset.UtcNow.AddMinutes(5);
        var msg = new TestMessage(Guid.NewGuid());

        await scheduler.SchedulePublish(msg, scheduledAt, TestContext.Current.CancellationToken);

        var call = Assert.Single(transport.Sends);
        Assert.Contains("SchedulingConsumer", call.Destination.AbsolutePath);
    }

    [Fact]
    public async Task SchedulePublish_WithConfigureDelegate_PreservesCustomHeaders()
    {
        var (scheduler, transport) = BuildScheduler();

        await scheduler.SchedulePublish(new TestMessage(Guid.NewGuid()),
            DateTimeOffset.UtcNow.AddMinutes(1),
            ctx => ctx.Headers["x-custom"] = "test-value",
            TestContext.Current.CancellationToken);

        Assert.Single(transport.Sends);
    }

    [Fact]
    public async Task SchedulePublish_WithTimeSpan_SendsCommand()
    {
        var (scheduler, transport) = BuildScheduler();
        var delay = TimeSpan.FromMinutes(10);

        await scheduler.SchedulePublish(new TestMessage(Guid.NewGuid()), delay,
            TestContext.Current.CancellationToken);

        Assert.Single(transport.Sends);
    }

    #endregion

    #region ScheduleSend - forwards command to scheduling consumer queue

    [Fact]
    public async Task ScheduleSend_ByConsumerType_SendsCommandToSchedulingConsumerQueue()
    {
        var (scheduler, transport) = BuildScheduler();

        await scheduler.ScheduleSend<TestConsumer, TestMessage>(
            new TestMessage(Guid.NewGuid()), DateTimeOffset.UtcNow.AddSeconds(30),
            TestContext.Current.CancellationToken);

        var call = Assert.Single(transport.Sends);
        Assert.Contains("SchedulingConsumer", call.Destination.AbsolutePath);
    }

    [Fact]
    public async Task ScheduleSend_ByQueueName_SendsCommandToSchedulingConsumerQueue()
    {
        var (scheduler, transport) = BuildScheduler();

        await scheduler.ScheduleSend("my-queue", new TestMessage(Guid.NewGuid()),
            DateTimeOffset.UtcNow.AddHours(1), TestContext.Current.CancellationToken);

        var call = Assert.Single(transport.Sends);
        Assert.Contains("SchedulingConsumer", call.Destination.AbsolutePath);
    }

    #endregion

    #region AddSchedulingConsumer - consumer is registered with the transport

    [Fact]
    public void AddSchedulingConsumer_RegistersConsumer()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        InterBusOptions? capturedOptions = null;
        services.AddInterBus(bus =>
        {
            bus.Services.AddSingleton<IInterBusTransport, NoOpTransport>();
            bus.AddSchedulingConsumer();
            capturedOptions = bus.Options;
        });

        Assert.NotNull(capturedOptions);
        var consumer = Assert.Single(capturedOptions.Consumers,
            c => c.ConsumerType.Name == "SchedulingConsumer");
        // SchedulePublishCommand and ScheduleSendCommand were merged into a single
        // ScheduleDeliveryCommand, so the consumer now handles exactly one message type.
        Assert.Single(consumer.MessageTypes);
        Assert.Equal("ScheduleDeliveryCommand", consumer.MessageTypes[0].Name);
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
        public List<(Uri Destination, byte[] Payload, MessageHeaders Headers)> Sends { get; } = [];

        public Task SendAsync(TransportSendContext context, CancellationToken cancellationToken)
        {
            Sends.Add((context.Destination, context.Payload, context.Headers));
            return Task.CompletedTask;
        }
    }

    #endregion
}
