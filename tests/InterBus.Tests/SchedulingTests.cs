using InterBus.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace InterBus.Tests;

/// <summary>
/// Unit tests for the <see cref="MessageScheduler"/> implementation of
/// <see cref="IMessageScheduler"/>.
/// </summary>
public sealed class SchedulingTests
{
    #region Helpers

    private static (IMessageScheduler Publisher, TestSchedulingProvider Provider) BuildPublisher(
        Action<IInterBusBuilder>? configureBus = null)
    {
        var schedulingProvider = new TestSchedulingProvider();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus =>
        {
            bus.Services.AddSingleton<IInterBusTransport, NoOpTransport>();
            bus.Services.AddSingleton<IMessageSchedulingProvider>(schedulingProvider);
            bus.AddScheduling();
            configureBus?.Invoke(bus);
        });

        var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IMessageScheduler>();
        return (publisher, schedulingProvider);
    }

    #endregion

    #region SchedulePublish — DateTimeOffset overload

    [Fact]
    public async Task SchedulePublish_WithDateTimeOffset_CallsSchedulingProviderForTopic()
    {
        var (publisher, schedulingProvider) = BuildPublisher();
        var scheduledAt = DateTimeOffset.UtcNow.AddMinutes(5);
        var msg = new TestMessage(Guid.NewGuid());

        await publisher.SchedulePublish(msg, scheduledAt, TestContext.Current.CancellationToken);

        var call = Assert.Single(schedulingProvider.Calls);
        Assert.True(call.IsTopic);
        Assert.Equal(scheduledAt, call.ScheduledAt);
        Assert.Contains(InterBusHeaders.MessageType, call.Headers.Keys);
    }

    [Fact]
    public async Task SchedulePublish_WithHeaders_PassesHeadersToProvider()
    {
        var (publisher, schedulingProvider) = BuildPublisher();
        var scheduledAt = DateTimeOffset.UtcNow.AddMinutes(1);
        var headers = new MessageHeaders { ["x-custom"] = "value" };

        await publisher.SchedulePublish(new TestMessage(Guid.NewGuid()), headers, scheduledAt,
            TestContext.Current.CancellationToken);

        var call = Assert.Single(schedulingProvider.Calls);
        Assert.Equal("value", call.Headers["x-custom"]);
    }

    #endregion

    #region SchedulePublish — TimeSpan overload (default interface impl)

    [Fact]
    public async Task SchedulePublish_WithTimeSpan_ComputesScheduledAtFromNow()
    {
        var (publisher, schedulingProvider) = BuildPublisher();
        var delay = TimeSpan.FromMinutes(10);
        var before = DateTimeOffset.UtcNow;

        await publisher.SchedulePublish(new TestMessage(Guid.NewGuid()), delay,
            TestContext.Current.CancellationToken);

        var after = DateTimeOffset.UtcNow;
        var call = Assert.Single(schedulingProvider.Calls);
        Assert.True(call.ScheduledAt >= before + delay);
        Assert.True(call.ScheduledAt <= after + delay);
    }

    #endregion

    #region ScheduleSend<TConsumer,T> — DateTimeOffset overload

    [Fact]
    public async Task ScheduleSend_ByConsumerType_CallsProviderForQueue()
    {
        var (publisher, schedulingProvider) = BuildPublisher();
        var scheduledAt = DateTimeOffset.UtcNow.AddSeconds(30);

        await publisher.ScheduleSend<TestConsumer, TestMessage>(
            new TestMessage(Guid.NewGuid()), scheduledAt, TestContext.Current.CancellationToken);

        var call = Assert.Single(schedulingProvider.Calls);
        Assert.False(call.IsTopic);
        Assert.Equal(scheduledAt, call.ScheduledAt);
        Assert.Equal(RoutingConventions.QueueName(typeof(TestConsumer)), call.Destination);
    }

    [Fact]
    public async Task ScheduleSend_ByConsumerType_WithHeaders_PassesHeaders()
    {
        var (publisher, schedulingProvider) = BuildPublisher();
        var headers = new MessageHeaders { ["x-trace"] = "abc" };

        await publisher.ScheduleSend<TestConsumer, TestMessage>(
            new TestMessage(Guid.NewGuid()), headers, DateTimeOffset.UtcNow.AddSeconds(5),
            TestContext.Current.CancellationToken);

        var call = Assert.Single(schedulingProvider.Calls);
        Assert.Equal("abc", call.Headers["x-trace"]);
    }

    #endregion

    #region ScheduleSend<T>(queueName) — DateTimeOffset overload

    [Fact]
    public async Task ScheduleSend_ByQueueName_CallsProviderForNamedQueue()
    {
        var (publisher, schedulingProvider) = BuildPublisher();
        var scheduledAt = DateTimeOffset.UtcNow.AddHours(1);

        await publisher.ScheduleSend("my-queue", new TestMessage(Guid.NewGuid()), scheduledAt,
            TestContext.Current.CancellationToken);

        var call = Assert.Single(schedulingProvider.Calls);
        Assert.False(call.IsTopic);
        Assert.Equal("my-queue", call.Destination);
        Assert.Equal(scheduledAt, call.ScheduledAt);
    }

    [Fact]
    public async Task ScheduleSend_ByQueueName_WithHeaders_PassesHeaders()
    {
        var (publisher, schedulingProvider) = BuildPublisher();
        var headers = new MessageHeaders { ["x-source"] = "unit-test" };

        await publisher.ScheduleSend("q", new TestMessage(Guid.NewGuid()), headers,
            DateTimeOffset.UtcNow.AddMinutes(2), TestContext.Current.CancellationToken);

        var call = Assert.Single(schedulingProvider.Calls);
        Assert.Equal("unit-test", call.Headers["x-source"]);
    }

    #endregion

    #region MessageType header always stamped

    [Fact]
    public async Task SchedulePublish_AlwaysStamps_MessageTypeHeader()
    {
        var (publisher, schedulingProvider) = BuildPublisher();

        await publisher.SchedulePublish(new TestMessage(Guid.NewGuid()),
            DateTimeOffset.UtcNow.AddSeconds(1), TestContext.Current.CancellationToken);

        var call = Assert.Single(schedulingProvider.Calls);
        Assert.True(call.Headers.ContainsKey(InterBusHeaders.MessageType));
        Assert.Contains(nameof(TestMessage), call.Headers[InterBusHeaders.MessageType]);
    }

    #endregion

    #region IMessageScheduler is separate from IMessagePublisher

    [Fact]
    public void IMessageScheduler_IsNotRegistered_WhenNotConfigured()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus =>
        {
            bus.Services.AddSingleton<IInterBusTransport, NoOpTransport>();
        });

        var provider = services.BuildServiceProvider();
        var schedulingPublisher = provider.GetService<IMessageScheduler>();

        Assert.Null(schedulingPublisher);
    }

    [Fact]
    public void IMessagePublisher_IsStillRegistered_WithoutScheduling()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus =>
        {
            bus.Services.AddSingleton<IInterBusTransport, NoOpTransport>();
        });

        var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var publisher = scope.ServiceProvider.GetService<IMessagePublisher>();

        Assert.NotNull(publisher);
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

    private sealed class TestSchedulingProvider : IMessageSchedulingProvider
    {
        public List<SchedulingCall> Calls { get; } = [];

        public Task SchedulePublishToTopicAsync(string topicName, byte[] payload, MessageHeaders headers,
            DateTimeOffset scheduledAt, CancellationToken cancellationToken)
        {
            Calls.Add(new SchedulingCall(topicName, true, payload, headers, scheduledAt));
            return Task.CompletedTask;
        }

        public Task ScheduleSendToQueueAsync(string queueName, byte[] payload, MessageHeaders headers,
            DateTimeOffset scheduledAt, CancellationToken cancellationToken)
        {
            Calls.Add(new SchedulingCall(queueName, false, payload, headers, scheduledAt));
            return Task.CompletedTask;
        }
    }

    private sealed record SchedulingCall(
        string Destination,
        bool IsTopic,
        byte[] Payload,
        MessageHeaders Headers,
        DateTimeOffset ScheduledAt);

    #endregion
}
