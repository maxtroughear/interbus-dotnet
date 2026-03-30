using System.Runtime.CompilerServices;
using InterBus.Extensions;
using InterBus.InMemory.Extensions;
using InterBus.Internal;
using InterBus.TickerQ.Extensions;
using Microsoft.Extensions.DependencyInjection;
using TickerQ.DependencyInjection;
using TickerQ.Utilities;
using TickerQ.Utilities.Entities;
using TickerQ.Utilities.Interfaces;
using TickerQ.Utilities.Interfaces.Managers;
using TickerQ.Utilities.Models;
using Xunit;

namespace InterBus.TickerQ.Tests;

/// <summary>
/// Unit tests for <see cref="TickerQMessageScheduler"/>, <see cref="ScheduledPublishRequest"/>,
/// and <see cref="ScheduledSendRequest"/>.
/// </summary>
public sealed class TickerQSchedulerTests
{
    #region ScheduledPublishRequest round-trip

    [Fact]
    public void ScheduledPublishRequest_RoundTrip_PreservesAllFields()
    {
        var payload = new byte[] { 1, 2, 3, 4, 5 };
        var original = new ScheduledPublishRequest
        {
            TopicName = "my-topic",
            Payload = payload,
            Headers = new Dictionary<string, string>
            {
                ["x-trace"] = "test",
                [InterBusHeaders.MessageType] = "MyMessage"
            }
        };

        var bytes = TickerHelper.CreateTickerRequest(original);
        var restored = TickerHelper.ReadTickerRequest<ScheduledPublishRequest>(bytes);

        Assert.Equal("my-topic", restored.TopicName);
        Assert.Equal(payload, restored.Payload);
        Assert.Equal("test", restored.Headers["x-trace"]);
        Assert.Equal("MyMessage", restored.Headers[InterBusHeaders.MessageType]);
    }

    #endregion

    #region ScheduledSendRequest round-trip

    [Fact]
    public void ScheduledSendRequest_RoundTrip_PreservesAllFields()
    {
        var payload = new byte[] { 42, 43, 44 };
        var original = new ScheduledSendRequest
        {
            QueueName = "my-queue",
            Payload = payload,
            Headers = new Dictionary<string, string> { ["x-custom"] = "val" }
        };

        var bytes = TickerHelper.CreateTickerRequest(original);
        var restored = TickerHelper.ReadTickerRequest<ScheduledSendRequest>(bytes);

        Assert.Equal("my-queue", restored.QueueName);
        Assert.Equal(payload, restored.Payload);
        Assert.Equal("val", restored.Headers["x-custom"]);
    }

    #endregion

    #region TickerQMessageScheduler — separate function names

    [Fact]
    public async Task ScheduleSendToQueueAsync_CreatesTimeTicker_WithSendFunctionName()
    {
        var manager = new StubTimeTickerManager();
        var scheduler = new TickerQMessageScheduler(manager, new NoOpHostScheduler());

        var scheduledAt = DateTimeOffset.UtcNow.AddHours(1);
        await scheduler.ScheduleSendToQueueAsync(
            "my-queue", [1, 2], new MessageHeaders(), scheduledAt,
            TestContext.Current.CancellationToken);

        var ticker = Assert.Single(manager.Added);
        Assert.Equal("interbus-send", ticker.Function);
        Assert.Equal(scheduledAt.UtcDateTime, ticker.ExecutionTime);
        Assert.NotNull(ticker.Request);

        var request = TickerHelper.ReadTickerRequest<ScheduledSendRequest>(ticker.Request!);
        Assert.Equal("my-queue", request.QueueName);
    }

    [Fact]
    public async Task SchedulePublishToTopicAsync_CreatesTimeTicker_WithPublishFunctionName()
    {
        var manager = new StubTimeTickerManager();
        var scheduler = new TickerQMessageScheduler(manager, new NoOpHostScheduler());

        var payload = new byte[] { 10, 20, 30 };
        var scheduledAt = DateTimeOffset.UtcNow.AddMinutes(5);

        await scheduler.SchedulePublishToTopicAsync("orders", payload, new MessageHeaders(), scheduledAt,
            TestContext.Current.CancellationToken);

        var ticker = Assert.Single(manager.Added);
        Assert.Equal("interbus-publish", ticker.Function);
        Assert.Equal(scheduledAt.UtcDateTime, ticker.ExecutionTime);

        var request = TickerHelper.ReadTickerRequest<ScheduledPublishRequest>(ticker.Request!);
        Assert.Equal("orders", request.TopicName);
        Assert.Equal(payload, request.Payload);
    }

    [Fact]
    public async Task SchedulePublishToTopicAsync_Headers_ArePersisted()
    {
        var manager = new StubTimeTickerManager();
        var scheduler = new TickerQMessageScheduler(manager, new NoOpHostScheduler());

        var headers = new MessageHeaders { [InterBusHeaders.MessageType] = "MyEvent" };
        await scheduler.SchedulePublishToTopicAsync("topic", [1], headers,
            DateTimeOffset.UtcNow.AddSeconds(10), TestContext.Current.CancellationToken);

        var ticker = Assert.Single(manager.Added);
        var request = TickerHelper.ReadTickerRequest<ScheduledPublishRequest>(ticker.Request!);
        Assert.Equal("MyEvent", request.Headers[InterBusHeaders.MessageType]);
    }

    [Fact]
    public async Task ScheduleSendToQueueAsync_NotifiesHostScheduler()
    {
        var manager = new StubTimeTickerManager();
        var hostScheduler = new TrackingHostScheduler();
        var scheduler = new TickerQMessageScheduler(manager, hostScheduler);

        await scheduler.ScheduleSendToQueueAsync(
            "q", [1], new MessageHeaders(), DateTimeOffset.UtcNow.AddSeconds(30),
            TestContext.Current.CancellationToken);

        Assert.Equal(1, hostScheduler.RestartIfNeededCallCount);
    }

    #endregion

    #region DI integration: AddTickerQScheduler registers IMessageSchedulingProvider and IMessageScheduler

    [Fact]
    public void AddTickerQScheduler_RegistersTickerQMessageScheduler_AsIMessageSchedulingProvider()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddScheduling();
            bus.AddTickerQScheduler();
        });
        services.AddTickerQ(options => options.DisableBackgroundServices());

        var provider = services.BuildServiceProvider();
        var schedulingProvider = provider.GetService<IMessageSchedulingProvider>();

        Assert.NotNull(schedulingProvider);
        Assert.IsType<TickerQMessageScheduler>(schedulingProvider);
    }

    [Fact]
    public void AddTickerQScheduler_RegistersMessageScheduler_AsIMessageScheduler()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddScheduling();
            bus.AddTickerQScheduler();
        });
        services.AddTickerQ(options => options.DisableBackgroundServices());

        var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var publisher = scope.ServiceProvider.GetService<IMessageScheduler>();

        Assert.NotNull(publisher);
        Assert.IsType<MessageScheduler>(publisher);
    }

    #endregion

    #region Support types

    private sealed class StubTimeTickerManager : ITimeTickerManager<TimeTickerEntity>
    {
        public List<TimeTickerEntity> Added { get; } = [];

        private static TickerResult<T> EmptyResult<T>() where T : class =>
            (TickerResult<T>)RuntimeHelpers.GetUninitializedObject(typeof(TickerResult<T>));

        public Task<TickerResult<TimeTickerEntity>> AddAsync(TimeTickerEntity entity,
            CancellationToken cancellationToken)
        {
            Added.Add(entity);
            return Task.FromResult(EmptyResult<TimeTickerEntity>());
        }

        public Task<TickerResult<TimeTickerEntity>> UpdateAsync(TimeTickerEntity timeTicker,
            CancellationToken cancellationToken) => Task.FromResult(EmptyResult<TimeTickerEntity>());

        public Task<TickerResult<TimeTickerEntity>> DeleteAsync(Guid id, CancellationToken cancellationToken)
            => Task.FromResult(EmptyResult<TimeTickerEntity>());

        public Task<TickerResult<List<TimeTickerEntity>>> AddBatchAsync(List<TimeTickerEntity> entities,
            CancellationToken cancellationToken) => Task.FromResult(EmptyResult<List<TimeTickerEntity>>());

        public Task<TickerResult<List<TimeTickerEntity>>> UpdateBatchAsync(List<TimeTickerEntity> timeTickers,
            CancellationToken cancellationToken) => Task.FromResult(EmptyResult<List<TimeTickerEntity>>());

        public Task<TickerResult<TimeTickerEntity>> DeleteBatchAsync(List<Guid> ids,
            CancellationToken cancellationToken) => Task.FromResult(EmptyResult<TimeTickerEntity>());
    }

    private sealed class NoOpHostScheduler : ITickerQHostScheduler
    {
        public bool IsRunning => false;

        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public void RestartIfNeeded(DateTime? dateTime) { }

        public void Restart() { }
    }

    private sealed class TrackingHostScheduler : ITickerQHostScheduler
    {
        public int RestartIfNeededCallCount { get; private set; }

        public bool IsRunning => false;

        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public void RestartIfNeeded(DateTime? dateTime) => RestartIfNeededCallCount++;

        public void Restart() { }
    }

    #endregion
}
