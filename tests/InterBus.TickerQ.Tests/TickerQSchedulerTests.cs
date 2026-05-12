using System.Runtime.CompilerServices;
using InterBus.Extensions;
using InterBus.InMemory.Extensions;
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
/// Unit tests for <see cref="TickerQMessageScheduler"/> and <see cref="ScheduledDeliveryRequest"/>.
/// </summary>
public sealed class TickerQSchedulerTests
{
    #region ScheduledDeliveryRequest round-trip

    [Fact]
    public void ScheduledDeliveryRequest_RoundTrip_PreservesAllFields()
    {
        var payload = new byte[] { 1, 2, 3, 4, 5 };
        var destination = new Uri("memorytopic://interbus/my-topic");
        var original = new ScheduledDeliveryRequest
        {
            Destination = destination.ToString(),
            Payload = payload,
            Headers = new Dictionary<string, string>
            {
                ["x-trace"] = "test",
                [InterBusHeaders.MessageType] = "MyMessage"
            }
        };

        var bytes = TickerHelper.CreateTickerRequest(original);
        var restored = TickerHelper.ReadTickerRequest<ScheduledDeliveryRequest>(bytes);

        Assert.Equal(destination.ToString(), restored.Destination);
        Assert.Equal(payload, restored.Payload);
        Assert.Equal("test", restored.Headers["x-trace"]);
        Assert.Equal("MyMessage", restored.Headers[InterBusHeaders.MessageType]);
    }

    #endregion

    #region TickerQMessageScheduler - single function name

    [Fact]
    public async Task ScheduleAsync_WithQueueUri_CreatesTimeTicker_WithDeliverFunctionName()
    {
        var manager = new StubTimeTickerManager();
        var scheduler = new TickerQMessageScheduler(manager);

        var scheduleId = Guid.CreateVersion7();
        var scheduledAt = DateTimeOffset.UtcNow.AddHours(1);
        var destination = new Uri("memoryqueue://interbus/my-queue");
        await scheduler.ScheduleAsync(
            scheduleId, destination, [1, 2], new MessageHeaders(), scheduledAt,
            TestContext.Current.CancellationToken);

        var ticker = Assert.Single(manager.Added);
        Assert.Equal("interbus-deliver", ticker.Function);
        Assert.Equal(scheduledAt.UtcDateTime, ticker.ExecutionTime);
        Assert.NotNull(ticker.Request);

        var request = TickerHelper.ReadTickerRequest<ScheduledDeliveryRequest>(ticker.Request!);
        Assert.Equal(destination.ToString(), request.Destination);

        // Ticker ID must equal the pre-generated schedule ID.
        Assert.Equal(scheduleId, ticker.Id);
    }

    [Fact]
    public async Task ScheduleAsync_WithTopicUri_CreatesTimeTicker_WithDeliverFunctionName()
    {
        var manager = new StubTimeTickerManager();
        var scheduler = new TickerQMessageScheduler(manager);

        var scheduleId = Guid.CreateVersion7();
        var payload = new byte[] { 10, 20, 30 };
        var scheduledAt = DateTimeOffset.UtcNow.AddMinutes(5);
        var destination = new Uri("memorytopic://interbus/orders");

        await scheduler.ScheduleAsync(scheduleId, destination, payload, new MessageHeaders(), scheduledAt,
            TestContext.Current.CancellationToken);

        var ticker = Assert.Single(manager.Added);
        Assert.Equal("interbus-deliver", ticker.Function);
        Assert.Equal(scheduledAt.UtcDateTime, ticker.ExecutionTime);

        var request = TickerHelper.ReadTickerRequest<ScheduledDeliveryRequest>(ticker.Request!);
        Assert.Equal(destination.ToString(), request.Destination);
        Assert.Equal(payload, request.Payload);
        Assert.Equal(scheduleId, ticker.Id);
    }

    [Fact]
    public async Task ScheduleAsync_Headers_ArePersisted()
    {
        var manager = new StubTimeTickerManager();
        var scheduler = new TickerQMessageScheduler(manager);

        var headers = new MessageHeaders { [InterBusHeaders.MessageType] = "MyEvent" };
        var destination = new Uri("memorytopic://interbus/topic");
        await scheduler.ScheduleAsync(Guid.CreateVersion7(), destination, [1], headers,
            DateTimeOffset.UtcNow.AddSeconds(10), TestContext.Current.CancellationToken);

        var ticker = Assert.Single(manager.Added);
        var request = TickerHelper.ReadTickerRequest<ScheduledDeliveryRequest>(ticker.Request!);
        Assert.Equal("MyEvent", request.Headers[InterBusHeaders.MessageType]);
    }

    [Fact]
    public async Task CancelAsync_WithValidId_CallsDeleteAsync()
    {
        var manager = new StubTimeTickerManager();
        var scheduler = new TickerQMessageScheduler(manager);

        var scheduleId = Guid.CreateVersion7();
        var destination = new Uri("memoryqueue://interbus/q");
        await scheduler.ScheduleAsync(
            scheduleId, destination, [1], new MessageHeaders(), DateTimeOffset.UtcNow.AddSeconds(30),
            TestContext.Current.CancellationToken);

        await scheduler.CancelAsync(scheduleId, TestContext.Current.CancellationToken);

        var deletedId = Assert.Single(manager.Deleted);
        Assert.Equal(scheduleId, deletedId);
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
            bus.AddScheduling(builder => builder.Directly().AddTickerQScheduler());
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
            bus.AddScheduling(builder => builder.Directly().AddTickerQScheduler());
        });
        services.AddTickerQ(options => options.DisableBackgroundServices());

        var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var publisher = scope.ServiceProvider.GetService<IScheduler>();

        Assert.NotNull(publisher);
    }

    #endregion

    #region Support types

    private sealed class StubTimeTickerManager : ITimeTickerManager<TimeTickerEntity>
    {
        public List<TimeTickerEntity> Added { get; } = [];
        public List<Guid> Deleted { get; } = [];

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
        {
            Deleted.Add(id);
            return Task.FromResult(EmptyResult<TimeTickerEntity>());
        }

        public Task<TickerResult<List<TimeTickerEntity>>> AddBatchAsync(List<TimeTickerEntity> entities,
            CancellationToken cancellationToken) => Task.FromResult(EmptyResult<List<TimeTickerEntity>>());

        public Task<TickerResult<List<TimeTickerEntity>>> UpdateBatchAsync(List<TimeTickerEntity> timeTickers,
            CancellationToken cancellationToken) => Task.FromResult(EmptyResult<List<TimeTickerEntity>>());

        public Task<TickerResult<TimeTickerEntity>> DeleteBatchAsync(List<Guid> ids,
            CancellationToken cancellationToken) => Task.FromResult(EmptyResult<TimeTickerEntity>());

        public Task<TickerResult<TimeTickerEntity>> AddAsync<TFunction>(DateTime? executionTime = null,
            CancellationToken cancellationToken = new()) where TFunction : class, ITickerFunction
            => Task.FromResult(EmptyResult<TimeTickerEntity>());

        public Task<TickerResult<TimeTickerEntity>> AddAsync<TFunction, TRequest>(DateTime? executionTime,
            TRequest request, CancellationToken cancellationToken = new())
            where TFunction : class, ITickerFunction<TRequest> => Task.FromResult(EmptyResult<TimeTickerEntity>());
    }

    #endregion
}
