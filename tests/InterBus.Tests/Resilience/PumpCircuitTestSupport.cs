using System.Collections.Concurrent;
using InterBus.Consuming;
using InterBus.Dispatching;
using InterBus.Extensions;
using InterBus.MessagePumps;
using InterBus.Resilience;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace InterBus.Tests.Resilience;

public sealed partial class PumpCircuitTests
{
    private static CancellationToken Token => TestContext.Current.CancellationToken;
    private static TaskCompletionSource Signal() => new(TaskCreationOptions.RunContinuationsAsynchronously);
    private static async Task UntilAsync(Func<bool> predicate)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(Token);
        deadline.CancelAfter(TimeSpan.FromSeconds(5));
        while (!predicate()) await Task.Delay(1, deadline.Token);
    }

    private sealed class TestConsumer : IConsumer;
    private sealed class Factory(IConsumerSource source) : IConsumerSourceFactory
    {
        public IConsumerSource Create(ConsumerDescriptor descriptor) => source;
    }
    private sealed class PumpTest : IAsyncDisposable
    {
        private readonly ServiceProvider _provider;
        private readonly ConsumerDescriptor _descriptor;
        private readonly PumpController _controller;
        private readonly List<TaskCompletionSource> _gates = [];
        public ManualClock Clock { get; } = new();
        public PumpLifetime Lifetime => _controller.Pumps.Single();
        public bool ExpectFailure;
        public PumpTest(IConsumerSource source, Func<DispatchContext, CancellationToken, Task>? dispatch = null,
            Action<PumpCircuitBreakerOptions>? configure = null, int concurrency = 1, int pumps = 1)
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddSingleton<TimeProvider>(Clock);
            services.AddSingleton<IConsumerSourceFactory>(new Factory(source));
            services.AddInterBus(bus => bus.RegisterConsumer<TestConsumer>(consumer =>
            {
                consumer.WithConcurrency(concurrency).WithPumpCount(pumps).UsePumpCircuitBreaker(options =>
                {
                    options.Dependency(policy =>
                    {
                        policy.MinimumThroughput = 1;
                        policy.BreakDuration = TimeSpan.FromSeconds(5);
                    });
                    options.ReopenJitter = TimeSpan.Zero;
                    options.ProbeTimeout = TimeSpan.FromSeconds(10);
                    configure?.Invoke(options);
                });
                consumer.Dispatch((context, _, _, token) => dispatch?.Invoke(context, token) ?? Task.CompletedTask);
            }));
            _provider = services.BuildServiceProvider();
            _descriptor = _provider.GetRequiredService<InterBusOptions>().Consumers.Single();
            _controller = _provider.GetRequiredService<PumpController>();
        }
        public async Task AdvanceToProbeAsync()
        {
            await UntilAsync(() => Clock.TimerCount > 0);
            Clock.Advance(TimeSpan.FromSeconds(5));
        }
        public Task StartAsync() => _controller.CreatePumpAsync(_descriptor, Token);
        public void Track(params TaskCompletionSource[] gates) => _gates.AddRange(gates);
        public TaskCompletionSource Gate() { var gate = Signal(); _gates.Add(gate); return gate; }
        public async ValueTask DisposeAsync()
        {
            foreach (var gate in _gates) gate.TrySetResult();
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            try { await _controller.StopAsync(deadline.Token); }
            catch when (ExpectFailure && Lifetime.Failure is not null) { }
            finally { await _provider.DisposeAsync(); }
        }
    }

    private sealed class Delivery(int id = 0) : IDelivery
    {
        public ReadOnlyMemory<byte> Payload => new byte[] { (byte)id };
        public MessageHeaders Headers { get; } = new();
        public Exception? DecodeError { get; init; }
        public bool Accept = true;
        public bool ThrowSettlement;
        public bool ThrowCleanup;
        public Task SettlementGate = Task.CompletedTask;
        public Task CleanupGate = Task.CompletedTask;
        public TaskCompletionSource Settling = Signal();
        public TaskCompletionSource Cleaning = Signal();
        public bool Disposed;
        public int Settlements;
        public DispatchResult? Result;
        public async Task<bool> SettleAsync(DispatchResult result, CancellationToken token)
        {
            Result = result;
            Interlocked.Increment(ref Settlements);
            Settling.TrySetResult();
            await SettlementGate.WaitAsync(token);
            if (ThrowSettlement) throw new IOException("settlement failed");
            return Accept;
        }
        public async ValueTask DisposeAsync()
        {
            Cleaning.TrySetResult();
            await CleanupGate;
            Disposed = true;
            if (ThrowCleanup) throw new IOException("cleanup failed");
        }
    }

    private sealed class BufferedSource(params Delivery[] deliveries) : IBufferedPollingSource
    {
        private readonly ConcurrentQueue<Delivery> _messages = new(deliveries);
        private TaskCompletionSource _changed = Signal();
        public int Calls;
        public int LastCapacity;
        public int PrefetchCount => 8;
        public int MaxBatchSize => 10;
        public void Enqueue(params Delivery[] messages)
        {
            foreach (var message in messages) _messages.Enqueue(message);
            Interlocked.Exchange(ref _changed, Signal()).TrySetResult();
        }
        public Task InitializeAsync(CancellationToken token) => Task.CompletedTask;
        public Task<PollResult> ReceiveOnceAsync(ReceiveBatch batch, CancellationToken token)
        {
            Interlocked.Increment(ref Calls);
            LastCapacity = batch.Capacity;
            var waiting = _changed.Task;
            var count = 0;
            while (count < batch.Capacity && _messages.TryDequeue(out var delivery)) { batch.Transfer(delivery); count++; }
            return Task.FromResult(new PollResult(count > 0, count == 0 ? async ct => await waiting.WaitAsync(ct) : null));
        }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class CallbackSource : IProbeControlledCallbackSource, IPollingFailureClassifier
    {
        public DeliveryHandler? Handler;
        public Task Active = Task.CompletedTask;
        public TaskCompletionSource Pausing = Signal();
        public int Probes;
        public int Resumes;
        private bool _paused;
        public Delivery Probe = new(2);
        public Exception? NextProbeError;
        public bool IsRecoverable(Exception error) => error is IOException;
        public Task InitializeAsync(CancellationToken token) => Task.CompletedTask;
        public Task StartAsync(DeliveryHandler handler, CancellationToken processing, CancellationToken operation)
        { Handler = handler; return Task.CompletedTask; }
        public async Task PauseAsync(CancellationToken token) { _paused = true; Pausing.TrySetResult(); await Active.WaitAsync(token); }
        public Task ResumeAsync(CancellationToken token) { _paused = false; Resumes++; return Task.CompletedTask; }
        public async Task<PollResult> ReceiveProbeAsync(CancellationToken processing, CancellationToken operation)
        {
            Assert.True(_paused);
            Interlocked.Increment(ref Probes);
            if (Interlocked.Exchange(ref NextProbeError, null) is { } error) throw error;
            await Handler!(Probe, processing);
            return new PollResult(true);
        }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class ScopedSource(Func<int, Delivery> delivery, Func<int, Task>? scope = null) : IScopedPollingSource
    {
        public int Iterations;
        public Task InitializeAsync(CancellationToken token) => Task.CompletedTask;
        public async Task<PollResult> ProcessOnceAsync(DeliveryHandler handler, CancellationToken intake, CancellationToken processing)
        {
            var index = Interlocked.Increment(ref Iterations);
            if (index > 2) return new PollResult(false, async ct => await Task.Delay(Timeout.Infinite, ct));
            await handler(delivery(index), processing);
            if (scope is not null) await scope(index);
            return new PollResult(true);
        }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class RetrySource : IBufferedPollingSource, IPollingRetryPolicy, IPollingFailureClassifier
    {
        public int Calls;
        public int Decisions;
        public bool Exhausted;
        public int PrefetchCount => 0;
        public int MaxBatchSize => 1;
        public Task InitializeAsync(CancellationToken token) => Task.CompletedTask;
        public Task<PollResult> ReceiveOnceAsync(ReceiveBatch batch, CancellationToken token)
        {
            if (Interlocked.Increment(ref Calls) == 1) throw new IOException("offline");
            return Task.FromResult(new PollResult(false, async ct => await Task.Delay(Timeout.Infinite, ct)));
        }
        public bool IsRecoverable(Exception error) => error is IOException;
        public TimeSpan? GetRetryDelay(Exception error, int attempt)
        { Interlocked.Increment(ref Decisions); return Exhausted ? null : TimeSpan.FromSeconds(2); }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class ManualClock : TimeProvider
    {
        private readonly object _gate = new();
        private DateTimeOffset _now = DateTimeOffset.Parse("2026-01-01T00:00:00Z");
        private readonly List<Timer> _timers = [];
        public int TimerCount { get { lock (_gate) return _timers.Count; } }
        public override DateTimeOffset GetUtcNow() { lock (_gate) return _now; }
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            var timer = new Timer(this, callback, state);
            lock (_gate) { _timers.Add(timer); timer.Change(dueTime, period); }
            return timer;
        }
        public void Advance(TimeSpan duration)
        {
            Timer[] timers;
            lock (_gate) { _now += duration; timers = _timers.ToArray(); }
            foreach (var timer in timers) timer.Fire();
        }
        private sealed class Timer(ManualClock clock, TimerCallback callback, object? state) : ITimer
        {
            private DateTimeOffset? _due;
            private TimeSpan _period;
            public bool Change(TimeSpan dueTime, TimeSpan period)
            {
                lock (clock._gate)
                {
                    _due = dueTime == Timeout.InfiniteTimeSpan ? null : clock._now + dueTime;
                    _period = period;
                }
                return true;
            }
            public void Fire()
            {
                lock (clock._gate)
                {
                    if (_due is null || _due > clock._now) return;
                    _due = _period == Timeout.InfiniteTimeSpan ? null : clock._now + _period;
                }
                callback(state);
            }
            public void Dispose() { lock (clock._gate) { _due = null; clock._timers.Remove(this); } }
            public ValueTask DisposeAsync() { Dispose(); return ValueTask.CompletedTask; }
        }
    }
}
