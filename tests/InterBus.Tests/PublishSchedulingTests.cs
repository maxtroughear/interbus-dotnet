using InterBus.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace InterBus.Tests;

/// <summary>
/// Unit tests for <see cref="PublishMessageScheduler"/> — the publish-based
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
            bus.Services.AddSingleton<IInterBusTransport>(transport);
            bus.AddPublishScheduling();
            bus.AddSchedulingConsumer();
            configureBus?.Invoke(bus);
        });

        var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var scheduler = scope.ServiceProvider.GetRequiredService<IMessageScheduler>();
        return (scheduler, transport);
    }

    #endregion

    #region AddPublishScheduling — registration

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
        services.AddInterBus(bus =>
        {
            bus.Services.AddSingleton<IInterBusTransport, NoOpTransport>();
        });

        var provider = services.BuildServiceProvider();
        var scheduler = provider.GetService<IMessageScheduler>();

        Assert.Null(scheduler);
    }

    #endregion

    #region SchedulePublish — forwards command to scheduling consumer queue

    [Fact]
    public async Task SchedulePublish_SendsCommandToSchedulingConsumerQueue()
    {
        var (scheduler, transport) = BuildScheduler();
        var scheduledAt = DateTimeOffset.UtcNow.AddMinutes(5);
        var msg = new TestMessage(Guid.NewGuid());

        await scheduler.SchedulePublish(msg, scheduledAt, TestContext.Current.CancellationToken);

        var call = Assert.Single(transport.Sends);
        Assert.Contains("SchedulingConsumer", call.QueueName);
    }

    [Fact]
    public async Task SchedulePublish_WithHeaders_PreservesCustomHeaders()
    {
        var (scheduler, transport) = BuildScheduler();
        var headers = new MessageHeaders { ["x-custom"] = "test-value" };

        await scheduler.SchedulePublish(new TestMessage(Guid.NewGuid()), headers,
            DateTimeOffset.UtcNow.AddMinutes(1), TestContext.Current.CancellationToken);

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

    #region ScheduleSend — forwards command to scheduling consumer queue

    [Fact]
    public async Task ScheduleSend_ByConsumerType_SendsCommandToSchedulingConsumerQueue()
    {
        var (scheduler, transport) = BuildScheduler();

        await scheduler.ScheduleSend<TestConsumer, TestMessage>(
            new TestMessage(Guid.NewGuid()), DateTimeOffset.UtcNow.AddSeconds(30),
            TestContext.Current.CancellationToken);

        var call = Assert.Single(transport.Sends);
        Assert.Contains("SchedulingConsumer", call.QueueName);
    }

    [Fact]
    public async Task ScheduleSend_ByQueueName_SendsCommandToSchedulingConsumerQueue()
    {
        var (scheduler, transport) = BuildScheduler();

        await scheduler.ScheduleSend("my-queue", new TestMessage(Guid.NewGuid()),
            DateTimeOffset.UtcNow.AddHours(1), TestContext.Current.CancellationToken);

        var call = Assert.Single(transport.Sends);
        Assert.Contains("SchedulingConsumer", call.QueueName);
    }

    #endregion

    #region AddSchedulingConsumer — consumer is registered with the transport

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
        Assert.Equal(2, consumer.MessageTypes.Count);
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
        public Task PublishToTopicAsync(string topicName, byte[] payload, MessageHeaders headers,
            CancellationToken cancellationToken) => Task.CompletedTask;

        public Task SendToQueueAsync(string queueName, byte[] payload, MessageHeaders headers,
            CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class CapturingTransport : IInterBusTransport
    {
        public List<(string TopicName, byte[] Payload, MessageHeaders Headers)> Publishes { get; } = [];
        public List<(string QueueName, byte[] Payload, MessageHeaders Headers)> Sends { get; } = [];

        public Task PublishToTopicAsync(string topicName, byte[] payload, MessageHeaders headers,
            CancellationToken cancellationToken)
        {
            Publishes.Add((topicName, payload, headers));
            return Task.CompletedTask;
        }

        public Task SendToQueueAsync(string queueName, byte[] payload, MessageHeaders headers,
            CancellationToken cancellationToken)
        {
            Sends.Add((queueName, payload, headers));
            return Task.CompletedTask;
        }
    }

    #endregion
}
