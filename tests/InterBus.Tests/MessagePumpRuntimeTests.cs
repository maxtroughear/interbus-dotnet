using InterBus.Consuming;
using InterBus.Dispatching;
using InterBus.Extensions;
using InterBus.MessagePumps;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace InterBus.Tests;

public class MessagePumpRuntimeTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task CompletedIntake_DoesNotHidePublishedFailure(bool buffered, bool canceledCompletion)
    {
        var (services, descriptor) = Create((_, _, _, _) => Task.CompletedTask);
        await using var provider = services.BuildServiceProvider();
        using var intake = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        intake.Cancel();
        var error = new IOException("source failed before intake completed");
        var source = new CompletedFaultSource(error, canceledCompletion);
        Exception? observed = null;
        var runtime = new ConsumerRuntime(descriptor, provider.GetRequiredService<DeliveryProcessor>(), failure => observed = failure);
        var failure = await Assert.ThrowsAsync<IOException>(() => buffered
            ? runtime.RunAsync((IBufferedSource)source, intake.Token, TestContext.Current.CancellationToken)
            : runtime.RunAsync((IManagedSource)source, intake.Token, TestContext.Current.CancellationToken));
        Assert.Same(error, failure);
        Assert.Same(error, observed);
    }

    [Theory]
    [InlineData(DispatchResultKind.Success)]
    [InlineData(DispatchResultKind.Release)]
    [InlineData(DispatchResultKind.DeadLetter)]
    public async Task Processor_SettlesOnce_AndDisposes(DispatchResultKind kind)
    {
        var result = kind switch
        {
            DispatchResultKind.Success => DispatchResult.Success,
            DispatchResultKind.Release => DispatchResult.Release(new Exception()),
            _ => DispatchResult.DeadLetter(DateTimeOffset.UtcNow, new Exception())
        };
        var (services, descriptor) = Create((ctx, _, _, _) => { ctx.Result = result; return Task.CompletedTask; });
        await using var provider = services.BuildServiceProvider();
        var delivery = new Delivery();
        await provider.GetRequiredService<DeliveryProcessor>().ProcessAsync(descriptor, delivery, TestContext.Current.CancellationToken);
        Assert.Same(result, delivery.Result);
        Assert.Equal(1, delivery.Settlements);
        Assert.True(delivery.Disposed);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task SettlementFailure_DoesNotAttemptConflictingSettlement(bool throws)
    {
        var (services, descriptor) = Create((_, _, _, _) => Task.CompletedTask);
        await using var provider = services.BuildServiceProvider();
        var delivery = new Delivery { ThrowSettlement = throws, Accepted = false };
        await provider.GetRequiredService<DeliveryProcessor>().ProcessAsync(descriptor, delivery, TestContext.Current.CancellationToken);
        Assert.Equal(1, delivery.Settlements);
        Assert.True(delivery.Disposed);
    }

    [Fact]
    public async Task DecodeFailure_ReleasesOwnedDelivery_AndCleansUp()
    {
        var (services, descriptor) = Create((_, _, _, _) => throw new Exception("Must not dispatch"));
        await using var provider = services.BuildServiceProvider();
        var delivery = new Delivery { DecodeError = new FormatException() };
        await provider.GetRequiredService<DeliveryProcessor>().ProcessAsync(descriptor, delivery, TestContext.Current.CancellationToken);
        Assert.Equal(DispatchResultKind.Release, delivery.Result?.Kind);
        Assert.True(delivery.Disposed);
    }

    [Fact]
    public async Task MultipleSources_ShareConcurrencyAndOwnershipBudget()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var finish = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var active = 0;
        var maximum = 0;
        var (services, descriptor) = Create(async (_, _, _, ct) =>
        {
            var count = Interlocked.Increment(ref active);
            InterlockedExtensionsMax(ref maximum, count);
            if (count == 2) entered.TrySetResult();
            try { await finish.Task.WaitAsync(ct); }
            finally { Interlocked.Decrement(ref active); }
        }, concurrency: 2, pumps: 4);
        await using var provider = services.BuildServiceProvider();
        var source = new Source(1);
        using var intake = new CancellationTokenSource();
        var run = new ConsumerRuntime(descriptor, provider.GetRequiredService<DeliveryProcessor>())
            .RunAsync(source, intake.Token, TestContext.Current.CancellationToken);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.InRange(source.Owned, 2, 3);
        Assert.Equal(2, maximum);
        await intake.CancelAsync();
        finish.SetResult();
        await run.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Equal(0, source.Owned);
    }

    [Fact]
    public async Task ManagedSource_AwaitsSettlementAndCleanup()
    {
        var (services, descriptor) = Create((_, _, _, _) => Task.CompletedTask);
        await using var provider = services.BuildServiceProvider();
        var source = new ManagedSource();
        await new ConsumerRuntime(descriptor, provider.GetRequiredService<DeliveryProcessor>())
            .RunAsync(source, TestContext.Current.CancellationToken, TestContext.Current.CancellationToken);
        Assert.True(source.Delivery.Disposed);
        Assert.Equal(1, source.Delivery.Settlements);
    }

    [Fact]
    public async Task OrderingKey_HoldsThroughSettlement_WhileOtherKeysProgress()
    {
        var firstSettling = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowSettlement = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var independent = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var second = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var (services, descriptor) = Create((ctx, _, _, _) =>
        {
            if (ctx.Headers["id"] == "second") second.SetResult();
            if (ctx.Headers["id"] == "independent") independent.SetResult();
            return Task.CompletedTask;
        }, concurrency: 2);
        await using var provider = services.BuildServiceProvider();
        var first = new Delivery { OrderingKey = "a", Settlement = async () =>
        { firstSettling.SetResult(); await allowSettlement.Task; } };
        var next = new Delivery { OrderingKey = "a" };
        next.Headers["id"] = "second";
        first.Headers["id"] = "first";
        var other = new Delivery { OrderingKey = "b" };
        other.Headers["id"] = "independent";
        using var intake = new CancellationTokenSource();
        var source = new ScriptedSource([first, next, other]);
        var run = new ConsumerRuntime(descriptor, provider.GetRequiredService<DeliveryProcessor>())
            .RunAsync(source, intake.Token, TestContext.Current.CancellationToken);
        await firstSettling.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        await independent.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.False(second.Task.IsCompleted);
        allowSettlement.SetResult();
        await second.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        await intake.CancelAsync();
        await run.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task EachDispatch_ReceivesFreshScope_AndDisposesIt()
    {
        var scopes = new List<ScopeMarker>();
        var (services, descriptor) = Create((_, _, sp, _) =>
        { scopes.Add(sp.GetRequiredService<ScopeMarker>()); return Task.CompletedTask; });
        services.AddScoped<ScopeMarker>();
        await using var provider = services.BuildServiceProvider();
        var processor = provider.GetRequiredService<DeliveryProcessor>();
        await processor.ProcessAsync(descriptor, new Delivery(), TestContext.Current.CancellationToken);
        await processor.ProcessAsync(descriptor, new Delivery(), TestContext.Current.CancellationToken);
        Assert.NotSame(scopes[0], scopes[1]);
        Assert.All(scopes, scope => Assert.True(scope.Disposed));
    }

    [Fact]
    public async Task CanceledProcessing_DisposesWithoutSettlement()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var (services, descriptor) = Create((_, _, _, ct) => Task.Delay(Timeout.Infinite, ct));
        await using var provider = services.BuildServiceProvider();
        var delivery = new Delivery();
        await provider.GetRequiredService<DeliveryProcessor>().ProcessAsync(descriptor, delivery, cancellation.Token);
        Assert.True(delivery.Disposed);
        Assert.Equal(0, delivery.Settlements);
    }

    private sealed class ScopeMarker : IDisposable
    {
        public bool Disposed;
        public void Dispose() => Disposed = true;
    }

    private sealed class ScriptedSource(Delivery[] deliveries) : IBufferedSource
    {
        public int PrefetchCount => 4;
        public Task InitializeAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        public async Task ReceiveAsync(DeliverySink sink, CancellationToken intakeToken)
        {
            foreach (var delivery in deliveries)
            {
                using var reservation = await sink.ReserveAsync(intakeToken);
                reservation.Handoff(delivery);
            }
            await Task.Delay(Timeout.Infinite, intakeToken);
        }
    }

    private static (ServiceCollection, ConsumerDescriptor) Create(
        Func<DispatchContext, ConsumePipelineExecutor, IServiceProvider, CancellationToken, Task> dispatch,
        int concurrency = 1, int pumps = 1)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var descriptor = new ConsumerDescriptor(typeof(TestConsumer), new Uri("memory://runtime"));
        new ConsumerBuilder<TestConsumer>(descriptor, services).Dispatch(dispatch).WithConcurrency(concurrency).WithPumpCount(pumps);
        services.AddInterBus(_ => { });
        return (services, descriptor);
    }

    private static void InterlockedExtensionsMax(ref int target, int value)
    {
        int current;
        do { current = Volatile.Read(ref target); if (current >= value) return; }
        while (Interlocked.CompareExchange(ref target, value, current) != current);
    }

    private sealed class TestConsumer : IConsumer;
    private sealed class Delivery : IDelivery
    {
        public ReadOnlyMemory<byte> Payload => ReadOnlyMemory<byte>.Empty;
        public MessageHeaders Headers { get; } = new();
        public Exception? DecodeError { get; init; }
        public string? OrderingKey { get; init; }
        public Func<Task>? Settlement { get; init; }
        public bool ThrowSettlement { get; init; }
        public bool Accepted { get; init; } = true;
        public int Settlements;
        public bool Disposed;
        public DispatchResult? Result;
        public Action? Cleanup;
        public async Task<bool> SettleAsync(DispatchResult result, CancellationToken cancellationToken)
        {
            Settlements++;
            Result = result;
            if (ThrowSettlement) throw new IOException("ambiguous settlement");
            if (Settlement is not null) await Settlement();
            return Accepted;
        }
        public ValueTask DisposeAsync() { Disposed = true; Cleanup?.Invoke(); return ValueTask.CompletedTask; }
    }
    private sealed class Source(int prefetch) : IBufferedSource
    {
        public int Owned;
        public int PrefetchCount => prefetch;
        public Task InitializeAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        public async Task ReceiveAsync(DeliverySink sink, CancellationToken intakeToken)
        {
            while (true)
            {
                using var reservation = await sink.ReserveAsync(intakeToken);
                Interlocked.Increment(ref Owned);
                var delivery = new Delivery { Cleanup = () => Interlocked.Decrement(ref Owned) };
                try { reservation.Handoff(delivery); }
                catch { await delivery.DisposeAsync(); throw; }
            }
        }
    }
    private sealed class ManagedSource : IManagedSource
    {
        public Delivery Delivery { get; } = new();
        public Task InitializeAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        public async Task RunAsync(Func<IDelivery, CancellationToken, Task> process, CancellationToken intakeToken,
            CancellationToken processingToken)
        {
            await process(Delivery, processingToken);
            Assert.True(Delivery.Disposed);
        }
    }

    private sealed class CompletedFaultSource(Exception error, bool canceledCompletion) : IManagedSource, IBufferedSource
    {
        public int PrefetchCount => 0;
        public Task<Exception> Failure { get; } = Task.FromResult(error);
        public Task InitializeAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        public Task ReceiveAsync(DeliverySink sink, CancellationToken intakeToken) => Complete(intakeToken);
        public Task RunAsync(Func<IDelivery, CancellationToken, Task> process, CancellationToken intakeToken,
            CancellationToken processingToken) => Complete(intakeToken);
        private Task Complete(CancellationToken token) => canceledCompletion ? Task.FromCanceled(token) : Task.CompletedTask;
    }
}
