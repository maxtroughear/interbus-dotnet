using InterBus.Generated;
using Microsoft.Extensions.Logging;
using InterBus.Extensions;
using InterBus.MessagePumps;
using InterBus.RabbitMQ.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Testcontainers.RabbitMq;
using System.Collections.Concurrent;

namespace InterBus.RabbitMQ.Tests;

[TestFixture]
public class ManagedShutdownTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task RealHost_DrainsCallbacksBeforeClosingChannel(bool recoverConnection)
    {
        await using var container = new RabbitMqBuilder("rabbitmq:4-alpine").Build();
        await container.StartAsync();
        var gate = new Gate();
        var log = new CaptureLog();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(gate);
        builder.Logging.AddProvider(log);
        builder.Services.AddInterBus(bus =>
        {
            bus.AddRabbitMqTransport(options => options.Uri = container.GetConnectionString());
            bus.AddConsumer<BlockingConsumer>(consumer => consumer.WithConcurrency(2));
        });
        using var host = builder.Build();
        await host.StartAsync();
        await using var scope = host.Services.CreateAsyncScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
        var messages = Enumerable.Range(0, 6).Select(i => new OrderPlaced(Guid.NewGuid(), "shutdown@test.com", i)).ToArray();
        foreach (var message in messages) await publisher.PublishAsync(message);
        await gate.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        if (recoverConnection)
        {
            var closed = await container.ExecAsync(["rabbitmqctl", "close_all_connections", "--global", "interbus recovery test"]);
            Assert.That(closed.ExitCode, Is.Zero, closed.Stderr);
            await WaitUntilAsync(async () =>
            {
                var connections = await container.ExecAsync(["rabbitmqctl", "list_connections", "--quiet", "--no-table-headers", "pid"]);
                return connections.Stdout.Split('\n', StringSplitOptions.RemoveEmptyEntries).Length >= 2;
            }, timeoutMs: 30_000, cancellationToken: TestContext.CurrentContext.CancellationToken);
            Assert.That(gate.Active, Is.EqualTo(2));
        }
        if (recoverConnection)
        {
            gate.Release.TrySetResult();
            await gate.AllReceived.Task.WaitAsync(TimeSpan.FromSeconds(30));
            var queueName = host.Services.GetRequiredService<InterBusOptions>().Consumers.Single().QueueName;
            await WaitUntilAsync(async () =>
            {
                var queues = await container.ExecAsync(["rabbitmqctl", "list_queues", "--quiet", "--no-table-headers",
                    "name", "messages_ready", "messages_unacknowledged"]);
                var row = queues.Stdout.Split('\n').Select(line => line.Trim().Split('\t'))
                    .SingleOrDefault(parts => parts.Length == 3 && parts[0] == queueName);
                return row is not null && row[1] == "0" && row[2] == "0";
            }, timeoutMs: 30_000, cancellationToken: TestContext.CurrentContext.CancellationToken);
            Assert.That(gate.Received.Keys, Is.EquivalentTo(messages.Select(message => message.OrderId)));
            Assert.That(log.Messages.Count(message => message.Contains("settlement rejected")), Is.GreaterThanOrEqualTo(2));
            Assert.That(gate.Maximum, Is.EqualTo(2));
            await host.StopAsync().WaitAsync(TimeSpan.FromSeconds(10));
        }
        else
        {
            var stop = host.StopAsync();
            try
            {
                Assert.That(stop.IsCompleted, Is.False);
                Assert.That(gate.Active, Is.EqualTo(2));
            }
            finally { gate.Release.TrySetResult(); }
            await stop.WaitAsync(TimeSpan.FromSeconds(10));
        }
        var pump = host.Services.GetRequiredService<PumpController>().Pumps.Single();
        Assert.That(pump.State, Is.EqualTo(PumpState.Stopped));
        Assert.That(pump.Failure, Is.Null);
        Assert.That(gate.Active, Is.Zero);
    }

    [Test]
    public async Task StopWhileBrokerUnavailable_RespectsDeadlineAndFinishesCallbacks()
    {
        await using var container = new RabbitMqBuilder("rabbitmq:4-alpine").Build();
        await container.StartAsync();
        var gate = new Gate();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(gate);
        builder.Services.AddInterBus(bus =>
        {
            bus.AddRabbitMqTransport(options => options.Uri = container.GetConnectionString());
            bus.AddConsumer<BlockingConsumer>(consumer => consumer.WithConcurrency(2));
        });
        using var host = builder.Build();
        await host.StartAsync();
        await using var scope = host.Services.CreateAsyncScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
        for (var i = 0; i < 2; i++) await publisher.PublishAsync(new OrderPlaced(Guid.NewGuid(), "outage", i));
        await gate.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await container.StopAsync();
        using var deadline = new CancellationTokenSource();
        var stop = host.StopAsync(deadline.Token);
        try
        {
            deadline.Cancel();
            try { await stop.WaitAsync(TimeSpan.FromSeconds(5)); }
            catch (OperationCanceledException) when (deadline.IsCancellationRequested) { }
            var pump = host.Services.GetRequiredService<PumpController>().Pumps.Single();
            try { await pump.Completion.WaitAsync(TimeSpan.FromSeconds(10)); }
            catch (Exception error) when (error is not TimeoutException) { }
            Assert.That(pump.Completion.IsCompleted, Is.True);
            Assert.That(gate.Active, Is.Zero);
            Assert.That(gate.Tokens.All(token => token.IsCancellationRequested), Is.True);
        }
        finally { gate.Release.TrySetResult(); }
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task PauseResume_RetainsSubscriptionAndSettlementResources(bool recoverWhilePaused)
    {
        await using var container = new RabbitMqBuilder("rabbitmq:4-alpine").Build();
        await container.StartAsync();
        var gate = new Gate();
        gate.Release.SetResult();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(gate);
        builder.Services.AddInterBus(bus =>
        {
            bus.AddRabbitMqTransport(options => options.Uri = container.GetConnectionString());
            bus.AddConsumer<BlockingConsumer>(consumer => consumer.WithConcurrency(2));
        });
        using var host = builder.Build();
        await host.StartAsync();
        var pump = host.Services.GetRequiredService<PumpController>().Pumps.Single();
        await using var scope = host.Services.CreateAsyncScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
        for (var i = 0; i < 3; i++)
        {
            await pump.PauseAsync().WaitAsync(TimeSpan.FromSeconds(10));
            var message = new OrderPlaced(Guid.NewGuid(), "pause-resume", i);
            await publisher.PublishAsync(message);
            if (recoverWhilePaused && i == 0)
            {
                var closed = await container.ExecAsync(["rabbitmqctl", "close_all_connections", "--global", "paused recovery test"]);
                Assert.That(closed.ExitCode, Is.Zero, closed.Stderr);
                await WaitUntilAsync(async () =>
                {
                    var connections = await container.ExecAsync(["rabbitmqctl", "list_connections", "--quiet", "--no-table-headers", "pid"]);
                    return connections.Stdout.Split('\n', StringSplitOptions.RemoveEmptyEntries).Length >= 2;
                }, timeoutMs: 30_000, cancellationToken: TestContext.CurrentContext.CancellationToken);
            }
            Assert.That(gate.Received.ContainsKey(message.OrderId), Is.False);
            await pump.ResumeAsync().WaitAsync(TimeSpan.FromSeconds(10));
            await pump.ResumeAsync().WaitAsync(TimeSpan.FromSeconds(10));
            await WaitUntilAsync(() => gate.Received.ContainsKey(message.OrderId),
                cancellationToken: TestContext.CurrentContext.CancellationToken);
        }
        await host.StopAsync().WaitAsync(TimeSpan.FromSeconds(10));
        Assert.That(gate.Received.Count, Is.EqualTo(3));
        Assert.That(gate.Received.Values, Is.All.EqualTo(1));
    }

    private sealed class CaptureLog : ILoggerProvider, ILogger
    {
        public ConcurrentQueue<string> Messages { get; } = new();
        public ILogger CreateLogger(string categoryName) => this;
        public bool IsEnabled(LogLevel level) => true;
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public void Log<TState>(LogLevel level, EventId id, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Messages.Enqueue(formatter(state, exception));
        public void Dispose() { }
    }

    public sealed class Gate
    {
        public int Active, Maximum;
        public ConcurrentDictionary<Guid, int> Received { get; } = new();
        public ConcurrentQueue<CancellationToken> Tokens { get; } = new();
        public TaskCompletionSource AllReceived { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
    public sealed class BlockingConsumer(Gate gate) : IConsumer<OrderPlaced>
    {
        public async Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            var active = Interlocked.Increment(ref gate.Active);
            int previous;
            do { previous = Volatile.Read(ref gate.Maximum); if (previous >= active) break; }
            while (Interlocked.CompareExchange(ref gate.Maximum, active, previous) != previous);
            gate.Tokens.Enqueue(cancellationToken);
            gate.Received.AddOrUpdate(message.OrderId, 1, (_, count) => count + 1);
            if (gate.Received.Count == 6) gate.AllReceived.TrySetResult();
            if (active == 2) gate.Entered.TrySetResult();
            try { await gate.Release.Task.WaitAsync(cancellationToken); }
            finally { Interlocked.Decrement(ref gate.Active); }
        }
    }
}
