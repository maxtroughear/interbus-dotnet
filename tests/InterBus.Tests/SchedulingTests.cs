using InterBus.Extensions;
using InterBus.Scheduler.Extensions;
using InterBus.Transport;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace InterBus.Tests;

public sealed class SchedulingTests
{
    #region Helpers

    private static (IScheduler Publisher, TestSchedulingProvider Provider) BuildPublisher(
        Action<IInterBusBuilder>? configureBus = null)
    {
        var schedulingProvider = new TestSchedulingProvider();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus =>
        {
            bus.Services.AddSingleton<IInterBusTransport, NoOpTransport>();
            bus.Services.AddSingleton<IMessageSchedulingProvider>(schedulingProvider);
            bus.AddScheduling(builder => builder.Directly());
            configureBus?.Invoke(bus);
        });

        var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IScheduler>();
        return (publisher, schedulingProvider);
    }

    #endregion

    #region SchedulePublish - DateTimeOffset overload

    [Fact]
    public async Task SchedulePublish_WithDateTimeOffset_CallsSchedulingProviderForTopic()
    {
        var (publisher, schedulingProvider) = BuildPublisher();
        var scheduledAt = DateTimeOffset.UtcNow.AddMinutes(5);
        var msg = new TestMessage();

        await publisher.SchedulePublishAsync(msg, scheduledAt, TestContext.Current.CancellationToken);

        var call = Assert.Single(schedulingProvider.Calls);
        Assert.Equal("topic", call.Destination.Scheme);
        Assert.Equal(scheduledAt, call.ScheduledAt);
        Assert.Contains(InterBusHeaders.MessageType, call.Headers.Keys);
    }

    [Fact]
    public async Task SchedulePublish_WithConfigureDelegate_PassesHeadersToProvider()
    {
        var (publisher, schedulingProvider) = BuildPublisher();
        var scheduledAt = DateTimeOffset.UtcNow.AddMinutes(1);

        await publisher.SchedulePublishAsync(new TestMessage(), scheduledAt,
            ctx => ctx.Headers["x-custom"] = "value",
            TestContext.Current.CancellationToken);

        var call = Assert.Single(schedulingProvider.Calls);
        Assert.Equal("value", call.Headers["x-custom"]);
    }

    #endregion

    #region SchedulePublish - TimeSpan overload (default interface impl)

    [Fact]
    public async Task SchedulePublish_WithTimeSpan_ComputesScheduledAtFromNow()
    {
        var (publisher, schedulingProvider) = BuildPublisher();
        var delay = TimeSpan.FromMinutes(10);
        var before = DateTimeOffset.UtcNow;

        await publisher.SchedulePublishAsync(new TestMessage(), delay,
            TestContext.Current.CancellationToken);

        var after = DateTimeOffset.UtcNow;
        var call = Assert.Single(schedulingProvider.Calls);
        Assert.True(call.ScheduledAt >= before + delay);
        Assert.True(call.ScheduledAt <= after + delay);
    }

    #endregion

    #region ScheduleSend<TConsumer,T> - DateTimeOffset overload

    [Fact]
    public async Task ScheduleSend_ByConsumerType_CallsProviderForQueue()
    {
        var (publisher, schedulingProvider) = BuildPublisher();
        var scheduledAt = DateTimeOffset.UtcNow.AddSeconds(30);

        await publisher.ScheduleSendAsync<TestConsumer, TestMessage>(
            new TestMessage(), scheduledAt, TestContext.Current.CancellationToken);

        var call = Assert.Single(schedulingProvider.Calls);
        Assert.Equal("queue", call.Destination.Scheme);
        Assert.Equal(scheduledAt, call.ScheduledAt);
        Assert.Equal(RoutingConventions.QueueName(typeof(TestConsumer)),
            RoutingConventions.GetDestinationName(call.Destination));
    }

    [Fact]
    public async Task ScheduleSend_ByConsumerType_WithConfigureDelegate_PassesHeaders()
    {
        var (publisher, schedulingProvider) = BuildPublisher();

        await publisher.ScheduleSendAsync<TestConsumer, TestMessage>(
            new TestMessage(), DateTimeOffset.UtcNow.AddSeconds(5),
            ctx => ctx.Headers["x-trace"] = "abc",
            TestContext.Current.CancellationToken);

        var call = Assert.Single(schedulingProvider.Calls);
        Assert.Equal("abc", call.Headers["x-trace"]);
    }

    #endregion

    #region ScheduleSend<T>(queueName) - DateTimeOffset overload

    [Fact]
    public async Task ScheduleSend_ByQueueName_CallsProviderForNamedQueue()
    {
        var (publisher, schedulingProvider) = BuildPublisher();
        var scheduledAt = DateTimeOffset.UtcNow.AddHours(1);

        await publisher.ScheduleSendAsync("my-queue", new TestMessage(), scheduledAt,
            TestContext.Current.CancellationToken);

        var call = Assert.Single(schedulingProvider.Calls);
        Assert.Equal("queue", call.Destination.Scheme);
        Assert.Equal("my-queue", RoutingConventions.GetDestinationName(call.Destination));
        Assert.Equal(scheduledAt, call.ScheduledAt);
    }

    [Fact]
    public async Task ScheduleSend_ByQueueName_WithConfigureDelegate_PassesHeaders()
    {
        var (publisher, schedulingProvider) = BuildPublisher();

        await publisher.ScheduleSendAsync("q", new TestMessage(),
            DateTimeOffset.UtcNow.AddMinutes(2),
            ctx => ctx.Headers["x-source"] = "unit-test",
            TestContext.Current.CancellationToken);

        var call = Assert.Single(schedulingProvider.Calls);
        Assert.Equal("unit-test", call.Headers["x-source"]);
    }

    #endregion

    #region MessageType header always stamped

    [Fact]
    public async Task SchedulePublish_AlwaysStamps_MessageTypeHeader()
    {
        var (publisher, schedulingProvider) = BuildPublisher();

        await publisher.SchedulePublishAsync(new TestMessage(),
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
        services.AddInterBus(bus => { bus.Services.AddSingleton<IInterBusTransport, NoOpTransport>(); });

        var provider = services.BuildServiceProvider();
        var schedulingPublisher = provider.GetService<IScheduler>();

        Assert.Null(schedulingPublisher);
    }

    [Fact]
    public void IMessagePublisher_IsStillRegistered_WithoutScheduling()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus => { bus.Services.AddSingleton<IInterBusTransport, NoOpTransport>(); });

        var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var publisher = scope.ServiceProvider.GetService<IPublisher>();

        Assert.NotNull(publisher);
    }

    #endregion

    #region Support types

    private sealed record TestMessage;

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

    private sealed class TestSchedulingProvider : IMessageSchedulingProvider
    {
        public List<SchedulingCall> Calls { get; } = [];

        public Task ScheduleAsync(Uri destination, byte[] payload, MessageHeaders headers,
            DateTimeOffset scheduledAt, CancellationToken cancellationToken)
        {
            Calls.Add(new SchedulingCall(destination, headers, scheduledAt));
            return Task.CompletedTask;
        }
    }

    private sealed record SchedulingCall(
        Uri Destination,
        MessageHeaders Headers,
        DateTimeOffset ScheduledAt);

    #endregion
}
