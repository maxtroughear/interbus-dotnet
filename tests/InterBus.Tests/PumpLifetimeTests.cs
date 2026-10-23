using InterBus.Consuming;
using InterBus.Extensions;
using InterBus.MessagePumps;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace InterBus.Tests;

public class PumpLifetimeTests
{
    [Fact]
    public async Task StopDuringInitialization_DoesNotStartIntake_AndDisposes()
    {
        await using var provider = Provider();
        var source = new Source { BlockInitialize = true };
        var lifetime = Lifetime(source, provider);
        var start = lifetime.StartAsync(TestContext.Current.CancellationToken);
        await source.Initializing.Task;
        var stop = lifetime.StopAsync(TestContext.Current.CancellationToken);
        await Task.WhenAll(start, stop).WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.False(source.Ran);
        Assert.True(source.Disposed);
        Assert.Equal(PumpState.Stopped, lifetime.State);
    }

    [Fact]
    public async Task InitializationFailure_IsRetained_AndDisposed()
    {
        await using var provider = Provider();
        var source = new Source { InitializationFailure = new IOException() };
        var lifetime = Lifetime(source, provider);
        await Assert.ThrowsAsync<IOException>(() => lifetime.StartAsync(TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<IOException>(() => lifetime.Completion);
        Assert.Equal(PumpState.Faulted, lifetime.State);
        Assert.Same(source.InitializationFailure, lifetime.Failure);
        Assert.True(source.Disposed);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task FaultOrUnexpectedCompletion_IsObserved(bool throws)
    {
        await using var provider = Provider();
        var source = new Source { FailRun = throws, CompleteRun = !throws };
        var lifetime = Lifetime(source, provider);
        try { await lifetime.StartAsync(TestContext.Current.CancellationToken); }
        catch (Exception error) when (error is IOException or InvalidOperationException) { }
        await Assert.ThrowsAnyAsync<Exception>(() => lifetime.Completion);
        Assert.Equal(PumpState.Faulted, lifetime.State);
        Assert.True(source.Disposed);
    }

    [Fact]
    public async Task ConcurrentStop_AwaitsSameShutdown()
    {
        await using var provider = Provider();
        var source = new Source();
        var lifetime = Lifetime(source, provider);
        await lifetime.StartAsync(TestContext.Current.CancellationToken);
        var first = lifetime.StopAsync(TestContext.Current.CancellationToken);
        var second = lifetime.StopAsync(TestContext.Current.CancellationToken);
        Assert.Same(first, second);
        await first;
        Assert.True(source.Disposed);
        Assert.Equal(PumpState.Stopped, lifetime.State);
    }

    [Fact]
    public async Task Deadline_CancelsProcessing_ButDoesNotDisposeActiveResources()
    {
        await using var provider = Provider();
        var source = new Source { IgnoreIntake = true };
        var lifetime = Lifetime(source, provider);
        await lifetime.StartAsync(TestContext.Current.CancellationToken);
        using var deadline = new CancellationTokenSource();
        var stop = lifetime.StopAsync(deadline.Token);
        deadline.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => stop);
        Assert.False(source.Disposed);
        Assert.True(source.ProcessingToken.IsCancellationRequested);
        source.Finish.SetResult();
        await lifetime.Completion;
        Assert.True(source.Disposed);
    }

    [Fact]
    public async Task RealHost_UsesNewRegistration_AndStopsSource()
    {
        var source = new Source();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddInterBus(bus =>
        {
            bus.Options.GetConsumerDescriptor(typeof(PumpLifetimeTests));
            bus.Services.AddSingleton<IConsumerSourceFactory>(new Factory(source));
        });
        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        var controller = host.Services.GetRequiredService<PumpController>();
        Assert.Single(controller.Pumps);
        Assert.True(source.Ran);
        var snapshot = controller.Pumps;
        await host.StopAsync(TestContext.Current.CancellationToken);
        Assert.True(source.Disposed);
        Assert.Equal(PumpState.Stopped, snapshot[0].State);
    }

    [Fact]
    public async Task BackgroundFault_IsReportedBeforeActiveWorkFinishes()
    {
        await using var provider = Provider();
        var source = new Source { IgnoreIntake = true };
        var lifetime = Lifetime(source, provider);
        await lifetime.StartAsync(TestContext.Current.CancellationToken);
        var failure = new IOException("listener failed");
        source.Health.SetResult(failure);
        Assert.Same(failure, await lifetime.Fault.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
        Assert.Equal(PumpState.Faulted, lifetime.State);
        Assert.False(source.Disposed);
        source.Finish.SetResult();
        await Assert.ThrowsAsync<IOException>(() => lifetime.Completion);
        Assert.True(source.Disposed);
    }

    [Fact]
    public async Task RealHost_PartialStartupFailure_StopsEveryInitializedConsumer()
    {
        var first = new Source();
        var second = new Source { InitializationFailure = new IOException("initialization failed") };
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddInterBus(bus =>
        {
            bus.Options.GetConsumerDescriptor(typeof(PumpLifetimeTests));
            bus.Options.GetConsumerDescriptor(typeof(Source));
            bus.Services.AddSingleton<IConsumerSourceFactory>(new SequenceFactory(first, second));
        });
        using var host = builder.Build();
        await Assert.ThrowsAsync<IOException>(() => host.StartAsync(TestContext.Current.CancellationToken));
        Assert.True(first.Disposed);
        Assert.True(second.Disposed);
        Assert.All(host.Services.GetRequiredService<PumpController>().Pumps,
            pump => Assert.True(pump.State is PumpState.Stopped or PumpState.Faulted));
    }

    private sealed class SequenceFactory(params Source[] sources) : IConsumerSourceFactory
    {
        private int _next;
        public IConsumerSource Create(ConsumerDescriptor descriptor) => sources[_next++];
    }

    [Fact]
    public async Task Startup_AwaitsAdapterReadiness_AndStopCancelsPendingStartup()
    {
        await using var provider = Provider();
        var source = new Source { BlockReady = true };
        var lifetime = Lifetime(source, provider);
        var start = lifetime.StartAsync(TestContext.Current.CancellationToken);
        Assert.True(source.Ran);
        Assert.False(start.IsCompleted);
        Assert.Equal(PumpState.Starting, lifetime.State);
        await Task.WhenAll(start, lifetime.StopAsync(TestContext.Current.CancellationToken))
            .WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.True(source.Disposed);
        Assert.Equal(PumpState.Stopped, lifetime.State);
    }

    [Fact]
    public async Task Controller_StopsEveryConsumer_AndAggregatesCleanupFailures()
    {
        await using var provider = Provider();
        var first = new Source { DisposalFailure = new IOException("first") };
        var second = new Source { DisposalFailure = new IOException("second") };
        var controller = new PumpController(new SequenceFactory(first, second), provider.GetRequiredService<DeliveryProcessor>());
        await controller.CreatePumpAsync(new ConsumerDescriptor(typeof(PumpLifetimeTests), new Uri("memory://first")),
            TestContext.Current.CancellationToken);
        var snapshot = controller.Pumps;
        await controller.CreatePumpAsync(new ConsumerDescriptor(typeof(Source), new Uri("memory://second")),
            TestContext.Current.CancellationToken);
        Assert.Single(snapshot);
        var failure = await Assert.ThrowsAsync<AggregateException>(() => controller.StopAsync(TestContext.Current.CancellationToken));
        Assert.Equal(2, failure.InnerExceptions.Count);
        Assert.True(first.Disposed);
        Assert.True(second.Disposed);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task LaterStopDeadline_CancelsProcessing_WithoutDisposingActiveResources(bool firstHasDeadline)
    {
        await using var provider = Provider();
        var source = new Source { IgnoreIntake = true };
        var lifetime = Lifetime(source, provider);
        await lifetime.StartAsync(TestContext.Current.CancellationToken);
        using var original = new CancellationTokenSource();
        using var later = new CancellationTokenSource();
        var first = lifetime.StopAsync(firstHasDeadline ? original.Token : CancellationToken.None);
        var second = lifetime.StopAsync(later.Token);
        try
        {
            later.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                second.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
            Assert.True(source.ProcessingToken.IsCancellationRequested);
            Assert.False(source.Disposed);
            Assert.False(lifetime.Completion.IsCompleted);
        }
        finally { source.Finish.TrySetResult(); }
        await first.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        await lifetime.DisposeAsync();
        Assert.True(source.Disposed);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RealHost_ObservesEveryFailure_WhenConsumersFaultTogetherOrDuringDrain(bool duringDrain)
    {
        var first = new Source();
        var second = new Source { IgnoreIntake = true };
        var firstFailure = new IOException("first consumer failed");
        var secondFailure = new IOException("second consumer failed");
        var builder = Host.CreateApplicationBuilder();
        builder.Services.Configure<HostOptions>(options =>
            options.BackgroundServiceExceptionBehavior = BackgroundServiceExceptionBehavior.Ignore);
        builder.Services.AddInterBus(bus =>
        {
            bus.Options.GetConsumerDescriptor(typeof(PumpLifetimeTests));
            bus.Options.GetConsumerDescriptor(typeof(Source));
            bus.Services.AddSingleton<IConsumerSourceFactory>(new SequenceFactory(first, second));
        });
        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        var pumpHost = (BackgroundService)host.Services.GetServices<IHostedService>()
            .Single(service => service.GetType().Name == "PumpHost");
        first.Health.SetResult(firstFailure);
        if (!duringDrain) second.Health.SetResult(secondFailure);
        await Assert.ThrowsAsync<IOException>(() => pumpHost.ExecuteTask!);
        var stop = host.StopAsync(TestContext.Current.CancellationToken);
        if (duringDrain) second.Health.SetResult(secondFailure);
        try
        {
            var observations = (Task<Exception?[]>)pumpHost.GetType().GetProperty("Observations",
                System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!.GetValue(pumpHost)!;
            var observed = await observations.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            Assert.Contains(firstFailure, observed);
            Assert.Contains(secondFailure, observed);
        }
        finally { second.Finish.TrySetResult(); }
        var shutdown = await Assert.ThrowsAsync<AggregateException>(() => stop);
        Assert.Equal(2, shutdown.Flatten().InnerExceptions.Count);
        Assert.All(host.Services.GetRequiredService<PumpController>().Pumps,
            pump => Assert.Equal(PumpState.Faulted, pump.State));
        Assert.True(first.Disposed);
        Assert.True(second.Disposed);
    }

    [Fact]
    public async Task RealHost_DeadlineAppliesAfterControllerAlreadyStartedGracefulStop()
    {
        var source = new Source { IgnoreIntake = true };
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddInterBus(bus =>
        {
            bus.Options.GetConsumerDescriptor(typeof(PumpLifetimeTests));
            bus.Services.AddSingleton<IConsumerSourceFactory>(new Factory(source));
        });
        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        var graceful = host.Services.GetRequiredService<PumpController>().StopAsync(CancellationToken.None);
        using var deadline = new CancellationTokenSource();
        var stop = host.StopAsync(deadline.Token);
        try
        {
            deadline.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                stop.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
            Assert.True(source.ProcessingToken.IsCancellationRequested);
            Assert.False(source.Disposed);
        }
        finally { source.Finish.TrySetResult(); }
        await graceful.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.True(source.Disposed);
    }

    private static ServiceProvider Provider()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(_ => { });
        return services.BuildServiceProvider();
    }
    private static PumpLifetime Lifetime(Source source, IServiceProvider provider) =>
        new(new ConsumerDescriptor(typeof(PumpLifetimeTests), new Uri("memory://test")), source,
            provider.GetRequiredService<DeliveryProcessor>());
    [Fact]
    public async Task CallbackControl_SerializesAndRejectsResumeAfterStop()
    {
        await using var provider = Provider();
        var source = new CallbackSource();
        var lifetime = new PumpLifetime(new ConsumerDescriptor(typeof(PumpLifetimeTests), new Uri("memory://callback")),
            source, provider.GetRequiredService<DeliveryProcessor>());
        await lifetime.StartAsync(TestContext.Current.CancellationToken);
        var pause = lifetime.PauseAsync(TestContext.Current.CancellationToken);
        await source.Pausing.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        var resume = lifetime.ResumeAsync(TestContext.Current.CancellationToken);
        var stop = lifetime.StopAsync(TestContext.Current.CancellationToken);
        Assert.False(source.Disposed);
        Assert.Equal(0, source.Resumes);
        source.Continue.TrySetResult();
        await pause;
        await Assert.ThrowsAsync<InvalidOperationException>(() => resume);
        await stop;
        Assert.True(source.Disposed);
        Assert.Equal(0, source.Resumes);
        Assert.Equal(PumpState.Stopped, lifetime.State);
    }

    [Fact]
    public async Task CallbackControl_RepeatedResumeDoesNotRestartActiveIntake()
    {
        await using var provider = Provider();
        var source = new CallbackSource();
        source.Continue.SetResult();
        await using var lifetime = new PumpLifetime(new ConsumerDescriptor(typeof(PumpLifetimeTests), new Uri("memory://callback")),
            source, provider.GetRequiredService<DeliveryProcessor>());
        await lifetime.StartAsync(TestContext.Current.CancellationToken);
        await lifetime.ResumeAsync(TestContext.Current.CancellationToken);
        for (var i = 0; i < 3; i++)
        {
            await lifetime.PauseAsync(TestContext.Current.CancellationToken);
            await lifetime.ResumeAsync(TestContext.Current.CancellationToken);
            await lifetime.ResumeAsync(TestContext.Current.CancellationToken);
        }
        Assert.Equal(1, source.Starts);
        Assert.Equal(3, source.Resumes);
    }

    [Fact]
    public async Task HybridExecution_IsRejectedBeforeInitialization_AndDisposedOnce()
    {
        await using var provider = Provider();
        var source = new HybridSource();
        var lifetime = new PumpLifetime(new ConsumerDescriptor(typeof(PumpLifetimeTests), new Uri("memory://invalid")),
            source, provider.GetRequiredService<DeliveryProcessor>());
        await Assert.ThrowsAsync<InvalidOperationException>(() => lifetime.StartAsync(TestContext.Current.CancellationToken));
        Assert.False(source.Initialized);
        Assert.Equal(1, source.Disposals);
        await Assert.ThrowsAsync<InvalidOperationException>(() => lifetime.DisposeAsync().AsTask());
        Assert.Equal(1, source.Disposals);
    }

    [Fact]
    public async Task CallbackStartup_TerminalCancellation_StopsWithoutFaulting()
    {
        await using var provider = Provider();
        var source = new CallbackSource { BlockStart = true };
        source.Continue.SetResult();
        await using var lifetime = new PumpLifetime(new ConsumerDescriptor(typeof(PumpLifetimeTests), new Uri("memory://callback")),
            source, provider.GetRequiredService<DeliveryProcessor>());
        var start = lifetime.StartAsync(TestContext.Current.CancellationToken);
        await source.Starting.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.False(start.IsCompleted);
        await Task.WhenAll(start, lifetime.StopAsync(TestContext.Current.CancellationToken)).WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Equal(PumpState.Stopped, lifetime.State);
        Assert.Null(lifetime.Failure);
        Assert.False(lifetime.Fault.IsCompleted);
        Assert.True(source.Disposed);
        Assert.True(source.Pausing.Task.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task CallbackPause_Cancellation_RemainsFatal()
    {
        await using var provider = Provider();
        var source = new CallbackSource();
        var lifetime = new PumpLifetime(new ConsumerDescriptor(typeof(PumpLifetimeTests), new Uri("memory://callback")),
            source, provider.GetRequiredService<DeliveryProcessor>());
        await lifetime.StartAsync(TestContext.Current.CancellationToken);
        using var operation = new CancellationTokenSource();
        var pause = lifetime.PauseAsync(operation.Token);
        await source.Pausing.Task;
        operation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pause);
        source.Continue.TrySetResult();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => lifetime.Completion);
        Assert.Equal(PumpState.Faulted, lifetime.State);
        Assert.IsAssignableFrom<OperationCanceledException>(lifetime.Failure);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => lifetime.DisposeAsync().AsTask());
    }

    [Fact]
    public async Task CallbackDefaultHealth_DoesNotRetainStoppedAdapters()
    {
        await using var provider = Provider();
        var references = StopCallback(provider);
        for (var i = 0; i < 3; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }
        Assert.All(references, reference => Assert.False(reference.IsAlive));
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private static WeakReference[] StopCallback(IServiceProvider provider)
    {
        var source = new CallbackSource();
        source.Continue.SetResult();
        var lifetime = new PumpLifetime(new ConsumerDescriptor(typeof(PumpLifetimeTests), new Uri("memory://callback")),
            source, provider.GetRequiredService<DeliveryProcessor>());
        lifetime.StartAsync(TestContext.Current.CancellationToken).GetAwaiter().GetResult();
        lifetime.DisposeAsync().AsTask().GetAwaiter().GetResult();
        return [new WeakReference(source), new WeakReference(lifetime)];
    }

    private sealed class HybridSource : IBufferedPollingSource, IScopedPollingSource
    {
        public bool Initialized;
        public int Disposals;
        public int PrefetchCount => 0;
        public int MaxBatchSize => 1;
        public Task InitializeAsync(CancellationToken token) { Initialized = true; return Task.CompletedTask; }
        public Task<PollResult> ReceiveOnceAsync(ReceiveBatch batch, CancellationToken token) => throw new NotSupportedException();
        public Task<PollResult> ProcessOnceAsync(DeliveryHandler process, CancellationToken intake, CancellationToken processing) => throw new NotSupportedException();
        public ValueTask DisposeAsync() { Disposals++; return ValueTask.CompletedTask; }
    }

    private sealed class CallbackSource : ICallbackSource
    {
        public int Starts, Resumes;
        public bool Disposed, BlockStart;
        private readonly TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public Task Ready => _ready.Task;
        public TaskCompletionSource Starting { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Pausing { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Continue { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public Task InitializeAsync(CancellationToken token) => Task.CompletedTask;
        public async Task StartAsync(DeliveryHandler process, CancellationToken processing, CancellationToken operation)
        {
            Starts++;
            Starting.TrySetResult();
            if (BlockStart) await Task.Delay(Timeout.Infinite, operation);
            _ready.TrySetResult();
        }
        public async Task PauseAsync(CancellationToken token) { Pausing.TrySetResult(); await Continue.Task.WaitAsync(token); }
        public Task ResumeAsync(CancellationToken token) { Resumes++; return Task.CompletedTask; }
        public ValueTask DisposeAsync() { Disposed = true; return ValueTask.CompletedTask; }
    }

    private sealed class Factory(Source source) : IConsumerSourceFactory
    {
        public IConsumerSource Create(ConsumerDescriptor descriptor) => source;
    }
    private sealed class Source : IScopedPollingSource
    {
        public bool BlockReady, BlockInitialize, FailRun, CompleteRun, IgnoreIntake, Ran, Disposed;
        public Exception? InitializationFailure, DisposalFailure;
        public CancellationToken ProcessingToken;
        private readonly TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public Task Ready => BlockReady ? _ready.Task : Task.CompletedTask;
        public TaskCompletionSource<Exception> Health { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public Task<Exception> Failure => Health.Task;
        public TaskCompletionSource Initializing { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Finish { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public async Task InitializeAsync(CancellationToken cancellationToken)
        {
            Initializing.SetResult();
            if (InitializationFailure is not null) throw InitializationFailure;
            if (BlockInitialize) await Task.Delay(Timeout.Infinite, cancellationToken);
        }
        public async Task<PollResult> ProcessOnceAsync(DeliveryHandler process, CancellationToken intakeToken,
            CancellationToken processingToken)
        {
            Ran = true;
            ProcessingToken = processingToken;
            if (FailRun) throw new IOException();
            if (CompleteRun) throw new InvalidOperationException("Source failed");
            if (IgnoreIntake) await Finish.Task;
            else await Task.Delay(Timeout.Infinite, intakeToken);
            return new PollResult(false);
        }
        public ValueTask DisposeAsync()
        {
            Disposed = true;
            return DisposalFailure is null ? ValueTask.CompletedTask : ValueTask.FromException(DisposalFailure);
        }
    }
}
