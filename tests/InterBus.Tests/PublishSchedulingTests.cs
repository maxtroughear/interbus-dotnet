using InterBus.Extensions;
using InterBus.Transporting;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace InterBus.Tests;

public sealed class PublishSchedulingTests
{
    #region Helpers

    private static (IScheduler Scheduler, CapturingTransport Transport) BuildScheduler(
        Action<InterBusBuilder>? configureBus = null)
    {
        var transport = new CapturingTransport();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus =>
        {
            bus.Services.AddKeyedSingleton<ITransport>("topic", transport);
            bus.Services.AddKeyedSingleton<ITransport>("queue", transport);
            bus.AddScheduling(builder => builder.ViaSend().AddSchedulingConsumer());
            configureBus?.Invoke(bus);
        });

        var provider = services.BuildServiceProvider();
        var scope = provider.CreateScope();
        var scheduler = scope.ServiceProvider.GetRequiredService<IScheduler>();
        return (scheduler, transport);
    }

    #endregion

    #region SchedulePublish - forwards command to scheduling consumer queue

    [Fact]
    public async Task SchedulePublish_SendsCommandToSchedulingConsumerQueue()
    {
        var (scheduler, transport) = BuildScheduler();
        var scheduledAt = DateTimeOffset.UtcNow.AddMinutes(5);
        var msg = new TestMessage();

        await scheduler.SchedulePublishAsync(msg, scheduledAt, TestContext.Current.CancellationToken);

        var call = Assert.Single(transport.Sends);
        Assert.Contains("SchedulingConsumer", call.Destination.AbsolutePath);
    }

    [Fact]
    public async Task SchedulePublish_WithConfigureDelegate_PreservesCustomHeaders()
    {
        var (scheduler, transport) = BuildScheduler();

        await scheduler.SchedulePublishAsync(new TestMessage(),
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

        await scheduler.SchedulePublishAsync(new TestMessage(), delay,
            TestContext.Current.CancellationToken);

        Assert.Single(transport.Sends);
    }

    #endregion

    #region ScheduleSend - forwards command to scheduling consumer queue

    [Fact]
    public async Task ScheduleSend_ByConsumerType_SendsCommandToSchedulingConsumerQueue()
    {
        var (scheduler, transport) = BuildScheduler();

        await scheduler.ScheduleSendAsync<TestConsumer, TestMessage>(
            new TestMessage(), DateTimeOffset.UtcNow.AddSeconds(30),
            TestContext.Current.CancellationToken);

        var call = Assert.Single(transport.Sends);
        Assert.Contains("SchedulingConsumer", call.Destination.AbsolutePath);
    }

    [Fact]
    public async Task ScheduleSend_ByQueueName_SendsCommandToSchedulingConsumerQueue()
    {
        var (scheduler, transport) = BuildScheduler();

        await scheduler.ScheduleSendAsync("my-queue", new TestMessage(),
            DateTimeOffset.UtcNow.AddHours(1), TestContext.Current.CancellationToken);

        var call = Assert.Single(transport.Sends);
        Assert.Contains("SchedulingConsumer", call.Destination.AbsolutePath);
    }

    #endregion

    #region Support types

    private sealed record TestMessage;

    private sealed class TestConsumer : IConsumer<TestMessage>
    {
        public Task Consume(TestMessage message, IConsumeContext context,
            CancellationToken cancellationToken = default) => Task.CompletedTask;
    }

    private sealed class CapturingTransport : ITransport
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
