using System.Runtime.CompilerServices;
using InterBus.Extensions;
using InterBus.InMemory.Extensions;
using InterBus.TickerQ.Extensions;
using Microsoft.Extensions.DependencyInjection;
using TickerQ.Utilities;
using TickerQ.Utilities.Entities;
using TickerQ.Utilities.Interfaces;
using TickerQ.Utilities.Interfaces.Managers;
using TickerQ.Utilities.Models;
using Xunit;

namespace InterBus.TickerQ.Tests;

/// <summary>
/// Unit tests for <see cref="TickerQMessageScheduler"/> and <see cref="ScheduledMessageRequest"/>.
/// </summary>
public sealed class TickerQSchedulerTests
{
    #region ScheduledMessageRequest round-trip

    [Fact]
    public void ScheduledMessageRequest_RoundTrip_PreservesAllFields()
    {
        var payload = new byte[] { 1, 2, 3, 4, 5 };
        var original = new ScheduledMessageRequest
        {
            Destination = "my-topic",
            IsTopic = true,
            Payload = payload,
            Headers = new Dictionary<string, string>
            {
                ["x-trace"] = "test",
                [InterBusHeaders.MessageType] = "MyMessage"
            }
        };

        var bytes = TickerHelper.CreateTickerRequest(original);
        var restored = TickerHelper.ReadTickerRequest<ScheduledMessageRequest>(bytes);

        Assert.Equal("my-topic", restored.Destination);
        Assert.True(restored.IsTopic);
        Assert.Equal(payload, restored.Payload);
        Assert.Equal("test", restored.Headers["x-trace"]);
        Assert.Equal("MyMessage", restored.Headers[InterBusHeaders.MessageType]);
    }

    [Fact]
    public void ScheduledMessageRequest_Queue_RoundTrip_PreservesIsTopic()
    {
        var original = new ScheduledMessageRequest
        {
            Destination = "my-queue",
            IsTopic = false,
            Payload = [42]
        };

        var bytes = TickerHelper.CreateTickerRequest(original);
        var restored = TickerHelper.ReadTickerRequest<ScheduledMessageRequest>(bytes);

        Assert.Equal("my-queue", restored.Destination);
        Assert.False(restored.IsTopic);
    }

    #endregion

    #region TickerQMessageScheduler

    [Fact]
    public async Task ScheduleSendToQueueAsync_CreatesTimeTicker_WithCorrectFunctionAndTime()
    {
        var manager = new StubTimeTickerManager();
        var scheduler = new TickerQMessageScheduler(manager, new NoOpHostScheduler());

        var scheduledAt = DateTimeOffset.UtcNow.AddHours(1);
        await scheduler.ScheduleSendToQueueAsync(
            "my-queue", [1, 2], new MessageHeaders(), scheduledAt,
            TestContext.Current.CancellationToken);

        var ticker = Assert.Single(manager.Added);
        Assert.Equal("interbus-deliver", ticker.Function);
        Assert.Equal(scheduledAt.UtcDateTime, ticker.ExecutionTime);
        Assert.NotNull(ticker.Request);

        var request = TickerHelper.ReadTickerRequest<ScheduledMessageRequest>(ticker.Request!);
        Assert.Equal("my-queue", request.Destination);
        Assert.False(request.IsTopic);
    }

    [Fact]
    public async Task SchedulePublishToTopicAsync_Request_ContainsTopicAndPayload()
    {
        var manager = new StubTimeTickerManager();
        var scheduler = new TickerQMessageScheduler(manager, new NoOpHostScheduler());

        var payload = new byte[] { 10, 20, 30 };
        var scheduledAt = DateTimeOffset.UtcNow.AddMinutes(5);

        await scheduler.SchedulePublishToTopicAsync("orders", payload, new MessageHeaders(), scheduledAt,
            TestContext.Current.CancellationToken);

        var ticker = Assert.Single(manager.Added);
        var request = TickerHelper.ReadTickerRequest<ScheduledMessageRequest>(ticker.Request!);
        Assert.Equal("orders", request.Destination);
        Assert.True(request.IsTopic);
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
        var request = TickerHelper.ReadTickerRequest<ScheduledMessageRequest>(ticker.Request!);
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

    #region DI integration: AddTickerQScheduler registers IMessageScheduler

    [Fact]
    public void AddTickerQScheduler_RegistersTickerQMessageScheduler_AsIMessageScheduler()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddTickerQScheduler(options => options.DisableBackgroundServices());
        });

        var provider = services.BuildServiceProvider();
        var scheduler = provider.GetService<IMessageScheduler>();

        Assert.NotNull(scheduler);
        Assert.IsType<TickerQMessageScheduler>(scheduler);
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
