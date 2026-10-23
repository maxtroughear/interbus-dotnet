using InterBus.Dispatching;
using InterBus.MessagePumps;
using InterBus.Resilience;
using Xunit;

namespace InterBus.Tests.Resilience;

public sealed partial class PumpCircuitTests
{
    private static Task FailFirst(DispatchContext context, CancellationToken token)
    {
        if (context.Payload[0] == 1) context.Result = DispatchResult.Release(new TimeoutException());
        return Task.CompletedTask;
    }

    [Fact]
    public async Task BufferedWorkers_DrainAcceptedBatch_BeforeOneBoundedProbe()
    {
        var entered = Signal();
        var release = Signal();
        var probeEntered = Signal();
        var finishProbe = Signal();
        var messages = Enumerable.Range(1, 3).Select(id => new Delivery(id)).ToArray();
        var source = new BufferedSource(messages);
        var calls = 0;
        await using var test = new PumpTest(source, async (context, token) =>
        {
            var count = Interlocked.Increment(ref calls);
            if (count == 1)
            {
                entered.TrySetResult();
                await release.Task.WaitAsync(token);
                context.Result = DispatchResult.Release(new IOException());
            }
            if (count == 4) { probeEntered.TrySetResult(); await finishProbe.Task.WaitAsync(token); }
        }, pumps: 3);
        test.Track(release, finishProbe);
        await test.StartAsync();
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5), Token);
        release.TrySetResult();
        await UntilAsync(() => messages.All(m => m.Disposed));
        Assert.Equal(3, calls);
        Assert.Equal("Open", test.Lifetime.Circuits.Single().State);
        var backlog = Enumerable.Range(4, 3).Select(id => new Delivery(id)).ToArray();
        source.Enqueue(backlog);
        await test.AdvanceToProbeAsync();
        await probeEntered.Task.WaitAsync(TimeSpan.FromSeconds(5), Token);
        Assert.Equal(1, source.LastCapacity);
        Assert.Equal(4, calls);
        finishProbe.TrySetResult();
        await UntilAsync(() => backlog.All(m => m.Disposed));
        await UntilAsync(() => test.Lifetime.Circuits.All(p => p.State == "Closed"));
    }

    [Fact]
    public async Task CallbackOpening_DrainsTriggeringCallback_WithoutCancelingProcessing()
    {
        var source = new CallbackSource();
        await using var test = new PumpTest(source, FailFirst);
        var settle = test.Gate();
        var initial = new Delivery(1) { SettlementGate = settle.Task };
        await test.StartAsync();
        source.Active = source.Handler!(initial, Token);
        await source.Pausing.Task.WaitAsync(TimeSpan.FromSeconds(5), Token);
        Assert.False(source.Active.IsCompleted);
        Assert.False(initial.Disposed);
        test.Clock.Advance(TimeSpan.FromSeconds(5));
        Assert.Equal(0, source.Probes);
        settle.TrySetResult();
        await source.Active;
        await UntilAsync(() => source.Resumes == 1);
        Assert.Equal(1, source.Probes);
        Assert.True(initial.Disposed);
        await test.Lifetime.StopAsync(Token);
        test.Clock.Advance(TimeSpan.FromDays(1));
        Assert.Equal(1, source.Resumes);
        Assert.Equal(0, test.Clock.TimerCount);
    }

    [Fact]
    public async Task Recovery_WaitsForSettlementDeliveryCleanupAndEnclosingScope()
    {
        var initial = new Delivery(1);
        var probe = new Delivery(2);
        var scopeEntered = Signal();
        TaskCompletionSource? scope = null;
        var source = new ScopedSource(index => index == 1 ? initial : probe, async index =>
        {
            if (index == 2) { scopeEntered.TrySetResult(); await scope!.Task; }
        });
        await using var test = new PumpTest(source, FailFirst);
        var settlement = test.Gate();
        var cleanup = test.Gate();
        scope = test.Gate();
        probe.SettlementGate = settlement.Task;
        probe.CleanupGate = cleanup.Task;
        await test.StartAsync();
        await UntilAsync(() => initial.Disposed);
        await test.AdvanceToProbeAsync();
        await probe.Settling.Task.WaitAsync(TimeSpan.FromSeconds(5), Token);
        Assert.Equal("HalfOpen", test.Lifetime.Circuits.Single().State);
        settlement.TrySetResult();
        await probe.Cleaning.Task.WaitAsync(TimeSpan.FromSeconds(5), Token);
        Assert.Equal("HalfOpen", test.Lifetime.Circuits.Single().State);
        cleanup.TrySetResult();
        await scopeEntered.Task.WaitAsync(TimeSpan.FromSeconds(5), Token);
        Assert.Equal("HalfOpen", test.Lifetime.Circuits.Single().State);
        scope.TrySetResult();
        await UntilAsync(() => test.Lifetime.Circuits.Single().State == "Closed");
        Assert.Equal(1, probe.Settlements);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RejectedOrFailedSettlement_DoesNotCloseDeliveryProbe(bool throws)
    {
        var initial = new Delivery(1);
        var probe = new Delivery(2) { Accept = false, ThrowSettlement = throws };
        var source = new BufferedSource(initial);
        await using var test = new PumpTest(source, FailFirst);
        await test.StartAsync();
        await UntilAsync(() => initial.Disposed);
        source.Enqueue(probe);
        await test.AdvanceToProbeAsync();
        await UntilAsync(() => probe.Disposed);
        await UntilAsync(() => test.Lifetime.Circuits.Single().State == "Open");
        Assert.Equal(DispatchResultKind.Success, probe.Result!.Kind);
        Assert.Equal(1, probe.Settlements);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DeliveryOrScopeCleanupFailure_CannotReportRecovery(bool scopeFails)
    {
        var initial = new Delivery(1);
        var probe = new Delivery(2) { ThrowCleanup = !scopeFails };
        var source = new ScopedSource(index => index == 1 ? initial : probe,
            index => index == 2 && scopeFails ? Task.FromException(new IOException("scope cleanup")) : Task.CompletedTask);
        await using var test = new PumpTest(source, FailFirst) { ExpectFailure = true };
        await test.StartAsync();
        await UntilAsync(() => initial.Disposed);
        await test.AdvanceToProbeAsync();
        await test.Lifetime.Fault.WaitAsync(TimeSpan.FromSeconds(5), Token);
        Assert.Equal(PumpState.Faulted, test.Lifetime.State);
        Assert.NotEqual("Closed", test.Lifetime.Circuits.Single().State);
        Assert.Equal(1, probe.Settlements);
    }

    [Fact]
    public async Task ProbeTimeout_RetainsOwnership_AndExcludesLateRecoveryEvidence()
    {
        var initial = new Delivery(1);
        var probe = new Delivery(2);
        var source = new BufferedSource(initial);
        TaskCompletionSource? release = null;
        await using var test = new PumpTest(source, async (context, token) =>
        {
            await FailFirst(context, token);
            if (context.Payload[0] == 2) await release!.Task.WaitAsync(token);
        }, options => options.ProbeDeliveryLimit = 3, pumps: 3);
        release = test.Gate();
        await test.StartAsync();
        await UntilAsync(() => initial.Disposed);
        source.Enqueue(probe);
        await test.AdvanceToProbeAsync();
        await UntilAsync(() => test.Lifetime.Circuits.Single().ActiveProbes == 1);
        test.Clock.Advance(TimeSpan.FromSeconds(10));
        var generation = test.Lifetime.Circuits.Single().Generation;
        Assert.False(probe.Disposed);
        test.Clock.Advance(TimeSpan.FromSeconds(5));
        Assert.Equal(1, test.Lifetime.Circuits.Single().ActiveProbes);
        release.TrySetResult();
        await UntilAsync(() => probe.Disposed);
        Assert.Equal(generation, test.Lifetime.Circuits.Single().Generation);
        Assert.NotEqual("Closed", test.Lifetime.Circuits.Single().State);
    }

    [Fact]
    public async Task EmptyDependencyProbe_DoesNotRecoverOrBusyPoll()
    {
        var initial = new Delivery(1);
        var source = new BufferedSource(initial);
        await using var test = new PumpTest(source, FailFirst);
        await test.StartAsync();
        await UntilAsync(() => initial.Disposed);
        var calls = source.Calls;
        await test.AdvanceToProbeAsync();
        await UntilAsync(() => source.Calls > calls);
        Assert.NotEqual("Closed", test.Lifetime.Circuits.Single().State);
        Assert.InRange(source.Calls - calls, 1, 2);
        var probe = new Delivery(2);
        source.Enqueue(probe);
        test.Clock.Advance(TimeSpan.FromSeconds(1));
        await UntilAsync(() => probe.Disposed && test.Lifetime.Circuits.Single().State == "Closed");
    }

    [Fact]
    public async Task ConfiguredProbeCount_IsSharedAcrossWorkers()
    {
        var initial = new Delivery(1);
        var source = new BufferedSource(initial);
        TaskCompletionSource? first = null, second = null;
        var entered = 0;
        await using var test = new PumpTest(source, async (context, token) =>
        {
            await FailFirst(context, token);
            if (context.Payload[0] is 2 or 3)
            {
                Interlocked.Increment(ref entered);
                await (context.Payload[0] == 2 ? first! : second!).Task.WaitAsync(token);
            }
        }, options => { options.ProbeDeliveryLimit = 2; options.SuccessfulProbesRequired = 2; }, concurrency: 2, pumps: 4);
        first = test.Gate(); second = test.Gate();
        await test.StartAsync();
        await UntilAsync(() => initial.Disposed);
        source.Enqueue(new Delivery(2), new Delivery(3));
        await test.AdvanceToProbeAsync();
        await UntilAsync(() => entered == 2);
        Assert.Equal(2, test.Lifetime.Circuits.Single().ActiveProbes);
        first.TrySetResult();
        await UntilAsync(() => test.Lifetime.Circuits.Single().ActiveProbes == 1);
        Assert.Equal("HalfOpen", test.Lifetime.Circuits.Single().State);
        second.TrySetResult();
        await UntilAsync(() => test.Lifetime.Circuits.Single().State == "Closed");
    }

    [Fact]
    public async Task ThrowingPredicate_DoesNotChangeSettlementOrOwnership()
    {
        var delivery = new Delivery(1);
        await using var test = new PumpTest(new BufferedSource(delivery), FailFirst,
            options => options.DependencyFailure = _ => throw new InvalidOperationException("predicate"));
        await test.StartAsync();
        await UntilAsync(() => delivery.Disposed);
        Assert.Equal(1, delivery.Settlements);
        Assert.Equal(DispatchResultKind.Release, delivery.Result!.Kind);
        Assert.Equal("Closed", test.Lifetime.Circuits.Single().State);
        Assert.Equal(PumpState.Running, test.Lifetime.State);
    }

    [Fact]
    public async Task DecodeError_IsExcludedFromDependencySampling()
    {
        var delivery = new Delivery(1) { DecodeError = new FormatException() };
        await using var test = new PumpTest(new BufferedSource(delivery), (_, _) => throw new Exception("must not dispatch"));
        await test.StartAsync();
        await UntilAsync(() => delivery.Disposed);
        Assert.Equal(1, delivery.Settlements);
        Assert.Equal("Closed", test.Lifetime.Circuits.Single().State);
    }

    [Fact]
    public async Task RetryAndCircuitDelays_ShareClock_AndEvaluateRetryOnce()
    {
        var source = new RetrySource();
        await using var test = new PumpTest(source, configure: options => options.DisableDependency().Transport(policy =>
        { policy.MinimumThroughput = 1; policy.BreakDuration = TimeSpan.FromSeconds(5); }));
        await test.StartAsync();
        await UntilAsync(() => source.Decisions == 1 && test.Clock.TimerCount == 1);
        test.Clock.Advance(TimeSpan.FromSeconds(2));
        Assert.Equal(1, source.Calls);
        test.Clock.Advance(TimeSpan.FromSeconds(3));
        await UntilAsync(() => source.Calls == 2 && test.Lifetime.Circuits.Single().State == "Closed");
        Assert.Equal(1, source.Decisions);
        Assert.Equal("Transport", test.Lifetime.Circuits.Single().Policy);
    }

    [Fact]
    public async Task ExhaustedRetryBudget_RemainsTerminal()
    {
        var source = new RetrySource { Exhausted = true };
        await using var test = new PumpTest(source, configure: options => options.DisableDependency().Transport(policy => policy.MinimumThroughput = 1))
        { ExpectFailure = true };
        await Assert.ThrowsAsync<IOException>(test.StartAsync);
        Assert.Equal(1, source.Decisions);
        Assert.Equal(1, source.Calls);
        Assert.Equal(PumpState.Faulted, test.Lifetime.State);
        Assert.Equal("Open", test.Lifetime.Circuits.Single().State);
    }

    [Fact]
    public async Task QueuedResume_RechecksCircuitBeforeRestartingCallbackIntake()
    {
        var entered = Signal();
        TaskCompletionSource? fail = null;
        var source = new CallbackSource();
        await using var test = new PumpTest(source, async (context, token) =>
        {
            entered.TrySetResult();
            await fail!.Task.WaitAsync(token);
            await FailFirst(context, token);
        });
        fail = test.Gate();
        var settle = test.Gate();
        var initial = new Delivery(1) { SettlementGate = settle.Task };
        await test.StartAsync();
        source.Active = source.Handler!(initial, Token);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5), Token);
        var pause = test.Lifetime.PauseAsync(Token);
        await source.Pausing.Task.WaitAsync(TimeSpan.FromSeconds(5), Token);
        var resume = test.Lifetime.ResumeAsync(Token);
        fail.TrySetResult();
        await initial.Settling.Task.WaitAsync(TimeSpan.FromSeconds(5), Token);
        Assert.Equal("Open", test.Lifetime.Circuits.Single().State);
        settle.TrySetResult();
        await Task.WhenAll(pause, resume);
        Assert.Equal(0, source.Resumes);
        await test.Lifetime.StopAsync(Token);
        test.Clock.Advance(TimeSpan.FromDays(1));
        Assert.Equal(0, source.Resumes);
    }
    [Fact]
    public async Task DependencyAndTransportPolicies_RecoverTogether()
    {
        var initial = new Delivery(1);
        var source = new CallbackSource();
        await using var test = new PumpTest(source, FailFirst, options => options.Transport(policy =>
        {
            policy.MinimumThroughput = 1;
            policy.FailureThreshold = 0.000001;
            policy.BreakDuration = TimeSpan.FromSeconds(5);
        }));
        await test.StartAsync();
        source.Active = source.Handler!(initial, Token);
        await source.Pausing.Task.WaitAsync(TimeSpan.FromSeconds(5), Token);
        await source.Active;
        source.NextProbeError = new IOException("receive offline");
        await test.AdvanceToProbeAsync();
        await UntilAsync(() => test.Lifetime.Circuits.Single(p => p.Policy == "Transport").State == "Open");
        Assert.False(source.Probe.Disposed);
        await test.AdvanceToProbeAsync();
        await UntilAsync(() => source.Probe.Disposed && test.Lifetime.Circuits.All(p => p.State == "Closed"));
    }

    [Fact]
    public async Task SamplingWindow_ExpiresOldFailuresBeforeEvaluatingThroughput()
    {
        var first = new Delivery(1);
        var source = new BufferedSource(first);
        await using var test = new PumpTest(source, (context, _) =>
        {
            context.Result = DispatchResult.Release(new IOException());
            return Task.CompletedTask;
        }, options => options.Dependency(policy =>
        {
            policy.MinimumThroughput = 2;
            policy.SamplingDuration = TimeSpan.FromSeconds(1);
        }));
        await test.StartAsync();
        await UntilAsync(() => first.Disposed);
        test.Clock.Advance(TimeSpan.FromSeconds(2));
        var second = new Delivery(2);
        source.Enqueue(second);
        await UntilAsync(() => second.Disposed);
        Assert.Equal("Closed", test.Lifetime.Circuits.Single().State);
        var third = new Delivery(3);
        source.Enqueue(third);
        await UntilAsync(() => third.Disposed);
        Assert.Equal("Open", test.Lifetime.Circuits.Single().State);
    }

    [Fact]
    public async Task ShutdownWhileOpen_CancelsWaitsAndPreventsLaterAcquisition()
    {
        var initial = new Delivery(1);
        var source = new BufferedSource(initial);
        await using var test = new PumpTest(source, FailFirst);
        await test.StartAsync();
        await UntilAsync(() => initial.Disposed);
        await test.Lifetime.StopAsync(Token);
        var calls = source.Calls;
        source.Enqueue(new Delivery(2));
        test.Clock.Advance(TimeSpan.FromDays(1));
        Assert.Equal(calls, source.Calls);
        Assert.Equal(PumpState.Stopped, test.Lifetime.State);
        Assert.Equal(0, test.Clock.TimerCount);
    }

    [Fact]
    public async Task PausedConsumer_DoesNotPauseIndependentConsumer()
    {
        var failed = new Delivery(1);
        var healthy = new Delivery(2);
        await using var paused = new PumpTest(new BufferedSource(failed), FailFirst);
        await using var running = new PumpTest(new BufferedSource(healthy));
        await paused.StartAsync();
        await UntilAsync(() => failed.Disposed);
        await running.StartAsync();
        await UntilAsync(() => healthy.Disposed);
        Assert.Equal("Open", paused.Lifetime.Circuits.Single().State);
        Assert.Equal("Closed", running.Lifetime.Circuits.Single().State);
        Assert.Equal(1, healthy.Settlements);
    }

    [Fact]
    public async Task PolicyEnablingAndDisabling_IsReflectedInLifetimeDiagnostics()
    {
        await using var test = new PumpTest(new BufferedSource(), configure: options => options
            .Transport().DisableTransport().DisableDependency().Transport());
        await test.StartAsync();
        Assert.Equal("Transport", test.Lifetime.Circuits.Single().Policy);
    }

    [Fact]
    public async Task CallbackWithoutBoundedCapability_FailsBeforeInitialization()
    {
        var source = new UnsupportedCallbackSource();
        await using var test = new PumpTest(source) { ExpectFailure = true };
        var error = await Assert.ThrowsAsync<InvalidOperationException>(test.StartAsync);
        Assert.Contains("bounded probe", error.Message);
        Assert.False(source.Initialized);
    }

    private sealed class UnsupportedCallbackSource : ICallbackSource
    {
        public bool Initialized;
        public Task InitializeAsync(CancellationToken token) { Initialized = true; return Task.CompletedTask; }
        public Task StartAsync(DeliveryHandler process, CancellationToken processing, CancellationToken operation) => Task.CompletedTask;
        public Task PauseAsync(CancellationToken token) => Task.CompletedTask;
        public Task ResumeAsync(CancellationToken token) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    [Fact]
    public async Task CircuitOpenedBeforeIdleWaitRegistration_DoesNotLoseRecoveryWakeup()
    {
        var first = new Delivery(1);
        var second = new Delivery(2);
        var source = new OpenDuringReceiveSource(first, second);
        await using var test = new PumpTest(source, FailFirst);
        await test.StartAsync();
        await UntilAsync(() => first.Disposed);
        await test.AdvanceToProbeAsync();
        Assert.Equal(0, source.OpenIdleCalls);
        await UntilAsync(() => second.Disposed && test.Lifetime.Circuits.Single().State == "Closed");
    }

    private sealed class OpenDuringReceiveSource(Delivery first, Delivery second) : IBufferedPollingSource
    {
        private int _calls;
        public int OpenIdleCalls;
        public int PrefetchCount => 0;
        public int MaxBatchSize => 1;
        public Task InitializeAsync(CancellationToken token) => Task.CompletedTask;
        public async Task<PollResult> ReceiveOnceAsync(ReceiveBatch batch, CancellationToken token)
        {
            switch (Interlocked.Increment(ref _calls))
            {
                case 1:
                    batch.Transfer(first);
                    await first.Settling.Task.WaitAsync(token);
                    return new PollResult(true, async ct =>
                    {
                        Interlocked.Increment(ref OpenIdleCalls);
                        await Task.Delay(Timeout.Infinite, ct);
                    });
                case 2:
                    batch.Transfer(second);
                    return new PollResult(true);
                default:
                    return new PollResult(false, async ct => await Task.Delay(Timeout.Infinite, ct));
            }
        }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

}
