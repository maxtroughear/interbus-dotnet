using System.Collections.Concurrent;
using System.Linq.Expressions;
using Hangfire;
using Hangfire.Common;
using Hangfire.InMemory;
using Hangfire.States;
using InterBus.Extensions;
using InterBus.Hangfire.Extensions;
using InterBus.InMemory.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace InterBus.Hangfire.Tests;

/// <summary>
/// Unit tests for <see cref="HangfireMessageScheduler"/> and <see cref="ScheduledDeliveryRequest"/>.
/// </summary>
public sealed class HangfireSchedulerTests
{
    [Fact]
    public async Task ScheduleAsync_WithQueueUri_CreatesScheduledJob_WithDeliverFunction()
    {
        var jobs = new StubBackgroundJobClient();
        var scheduler = new HangfireMessageScheduler(jobs);

        var destination = new Uri("memoryqueue://interbus/my-queue");
        var payload = new byte[] { 1, 2, 3 };
        var scheduledAt = DateTimeOffset.UtcNow.AddMinutes(3);
        var headers = new MessageHeaders { [InterBusHeaders.MessageType] = "MyMessage" };

        var jobId = await scheduler.ScheduleAsync(destination, payload, headers, scheduledAt,
            TestContext.Current.CancellationToken);

        var created = Assert.Single(jobs.CreatedJobs);
        Assert.Equal(jobId, created.JobId);
        Assert.Equal(nameof(HangfireDeliverFunction.InvokeAsync), created.Job.Method.Name);
        var state = Assert.IsType<ScheduledState>(created.State);
        Assert.Equal(scheduledAt.UtcDateTime, state.EnqueueAt);

        var request = Assert.IsType<ScheduledDeliveryRequest>(created.Job.Args[0]);
        Assert.Equal(destination.ToString(), request.Destination);
        Assert.Equal(payload, request.Payload);
        Assert.Equal("MyMessage", request.Headers[InterBusHeaders.MessageType]);
    }

    [Fact]
    public async Task CancelAsync_WithScheduledId_DeletesCreatedHangfireJob()
    {
        var jobs = new StubBackgroundJobClient();
        var scheduler = new HangfireMessageScheduler(jobs);

        var jobId = await scheduler.ScheduleAsync(
            new Uri("memorytopic://interbus/orders"),
            [10],
            new MessageHeaders(),
            DateTimeOffset.UtcNow.AddSeconds(30),
            TestContext.Current.CancellationToken);

        var created = Assert.Single(jobs.CreatedJobs);
        var deletedBeforeCancel = jobs.DeletedJobIds.Count;

        await scheduler.CancelAsync(jobId, TestContext.Current.CancellationToken);

        var deletedJobId = Assert.Single(jobs.DeletedJobIds.Skip(deletedBeforeCancel));
        Assert.Equal(created.JobId, deletedJobId);
    }

    [Fact]
    public void AddHangfireScheduler_RegistersHangfireMessageScheduler_AsIMessageSchedulingProvider()
    {
        var services = new ServiceCollection();
        services.AddInterBus(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddScheduling(builder => builder.Directly().AddHangfireScheduler());
        });
        services.AddSingleton<IBackgroundJobClient, StubBackgroundJobClient>();

        var provider = services.BuildServiceProvider();
        var schedulingProvider = provider.GetService<IMessageSchedulingProvider>();

        Assert.NotNull(schedulingProvider);
        Assert.IsType<HangfireMessageScheduler>(schedulingProvider);
    }

    [Fact]
    public void AddHangfireScheduler_RegistersMessageScheduler_AsIMessageScheduler()
    {
        var services = new ServiceCollection();
        services.AddInterBus(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddScheduling(builder => builder.Directly().AddHangfireScheduler());
        });
        services.AddSingleton<IBackgroundJobClient, StubBackgroundJobClient>();

        var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var scheduler = scope.ServiceProvider.GetService<IScheduler>();

        Assert.NotNull(scheduler);
    }

    [Fact]
    public async Task SchedulePublishAsync_MessageIsConsumed_UsingHangfireInMemoryStorage()
    {
        var storage = new InMemoryStorage();
        var client = new BackgroundJobClient(storage);
        var tracker = new IntegrationMessageTracker();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(tracker);
        services.AddSingleton<IBackgroundJobClient>(client);
        services.AddInterBus(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddScheduling(builder => builder.Directly().AddHangfireScheduler());
            bus.RegisterConsumer<IntegrationMessageConsumer>(c =>
                c.Consumes<IntegrationMessageConsumer, IntegrationMessage>());
        });

        var provider = services.BuildServiceProvider();
        var cts = new CancellationTokenSource();

        foreach (var svc in provider.GetServices<IHostedService>())
            await svc.StartAsync(cts.Token);

        var serverOptions = new BackgroundJobServerOptions
        {
            Activator = new ServiceProviderJobActivator(provider),
            SchedulePollingInterval = TimeSpan.FromMilliseconds(100),
            WorkerCount = 1
        };
        var server = new BackgroundJobServer(serverOptions, storage);

        try
        {
            using var scope = provider.CreateScope();
            var scheduler = scope.ServiceProvider.GetRequiredService<IScheduler>();

            var message = new IntegrationMessage(Guid.NewGuid(), "hello-hangfire");
            await scheduler.SchedulePublishAsync(
                message, DateTimeOffset.UtcNow.AddSeconds(-1), cts.Token);

            await WaitUntilAsync(() => tracker.Messages.Count >= 1, timeoutMs: 5000);

            var delivered = Assert.Single(tracker.Messages);
            Assert.Equal(message.Id, delivered.Id);
            Assert.Equal(message.Payload, delivered.Payload);
        }
        finally
        {
            server.Dispose();
            await cts.CancelAsync();
            foreach (var svc in provider.GetServices<IHostedService>())
            {
                try { await svc.StopAsync(CancellationToken.None); }
                catch (OperationCanceledException) { }
            }
            await provider.DisposeAsync();
            cts.Dispose();
        }
    }

    private static async Task WaitUntilAsync(Func<bool> condition, int timeoutMs = 5000, int intervalMs = 25)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (!condition() && DateTime.UtcNow < deadline)
            await Task.Delay(intervalMs);
    }

    internal record IntegrationMessage(Guid Id, string Payload);

    internal sealed class IntegrationMessageTracker
    {
        private readonly ConcurrentBag<IntegrationMessage> _messages = [];
        public IReadOnlyCollection<IntegrationMessage> Messages => _messages;
        public void Track(IntegrationMessage msg) => _messages.Add(msg);
    }

    internal sealed class IntegrationMessageConsumer(IntegrationMessageTracker tracker)
        : IConsumer<IntegrationMessage>
    {
        public Task Consume(IntegrationMessage message, IConsumeContext context,
            CancellationToken cancellationToken = default)
        {
            tracker.Track(message);
            return Task.CompletedTask;
        }
    }

    private sealed class ServiceProviderJobActivator(IServiceProvider serviceProvider) : JobActivator
    {
        public override JobActivatorScope BeginScope(JobActivatorContext context) =>
            new ServiceScopeActivatorScope(serviceProvider.CreateScope());

        private sealed class ServiceScopeActivatorScope(IServiceScope scope) : JobActivatorScope
        {
            public override object Resolve(Type type) =>
                scope.ServiceProvider.GetRequiredService(type);

            public override void DisposeScope() => scope.Dispose();
        }
    }

    private sealed class StubBackgroundJobClient : IBackgroundJobClient
    {
        private int _jobId = 0;

        public List<(string JobId, Job Job, IState State)> CreatedJobs { get; } = [];
        public List<string> DeletedJobIds { get; } = [];

        public string Create(Job job, IState state)
        {
            var jobId = Interlocked.Increment(ref _jobId).ToString();
            CreatedJobs.Add((jobId, job, state));
            return jobId;
        }

        public bool ChangeState(string jobId, IState state, string expectedState)
        {
            if (state is DeletedState)
                DeletedJobIds.Add(jobId);

            return true;
        }
    }
}
