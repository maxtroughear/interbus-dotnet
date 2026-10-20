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
        await Task.WhenAll(start, stop).WaitAsync(TimeSpan.FromSeconds(5));
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
        await lifetime.StartAsync(TestContext.Current.CancellationToken);
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
    private sealed class Factory(Source source) : IConsumerSourceFactory
    {
        public IConsumerSource Create(ConsumerDescriptor descriptor) => source;
    }
    private sealed class Source : IManagedSource
    {
        public bool BlockInitialize, FailRun, CompleteRun, IgnoreIntake, Ran, Disposed;
        public Exception? InitializationFailure;
        public CancellationToken ProcessingToken;
        public TaskCompletionSource Initializing { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Finish { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public async Task InitializeAsync(CancellationToken cancellationToken)
        {
            Initializing.SetResult();
            if (InitializationFailure is not null) throw InitializationFailure;
            if (BlockInitialize) await Task.Delay(Timeout.Infinite, cancellationToken);
        }
        public async Task RunAsync(Func<IDelivery, CancellationToken, Task> process, CancellationToken intakeToken,
            CancellationToken processingToken)
        {
            Ran = true;
            ProcessingToken = processingToken;
            if (FailRun) throw new IOException();
            if (CompleteRun) return;
            if (IgnoreIntake) await Finish.Task;
            else await Task.Delay(Timeout.Infinite, intakeToken);
        }
        public ValueTask DisposeAsync() { Disposed = true; return ValueTask.CompletedTask; }
    }
}
