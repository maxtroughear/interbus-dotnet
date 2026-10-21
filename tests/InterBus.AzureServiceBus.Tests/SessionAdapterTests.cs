using Azure.Messaging.ServiceBus;
using InterBus.Consuming;
using InterBus.Dispatching;
using InterBus.Extensions;
using InterBus.MessagePumps;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace InterBus.AzureServiceBus.Tests;

[TestFixture]
public partial class SessionAdapterTests
{
    [TestCase(DispatchResultKind.Success)]
    [TestCase(DispatchResultKind.Release)]
    [TestCase(DispatchResultKind.DeadLetter)]
    public async Task SessionStop_PreservesCallbackThroughSettlement(DispatchResultKind kind)
    {
        using var sdkStop = new CancellationTokenSource();
        var processor = new SessionProcessor { SdkStop = sdkStop };
        await using var clients = new AzureServiceBusClientProvider(new Client(processor), new Admin());
        await using var source = Source(clients);
        await source.InitializeAsync(default);
        using var intake = new CancellationTokenSource();
        var receiver = new Receiver();
        var entered = Signal();
        var finish = Signal();
        var run = source.RunAsync(async (delivery, token) =>
        {
            await using (delivery)
            {
                entered.SetResult();
                await finish.Task.WaitAsync(token);
                var result = kind switch
                {
                    DispatchResultKind.Success => DispatchResult.Success,
                    DispatchResultKind.Release => DispatchResult.Release(new IOException()),
                    _ => DispatchResult.DeadLetter(DateTimeOffset.UtcNow, new IOException())
                };
                Assert.That(await delivery.SettleAsync(result, token), Is.True);
            }
        }, intake.Token, default);
        processor.Active = processor.DeliverAsync(Args(receiver, sdkStop.Token));
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        try
        {
            await intake.CancelAsync();
            await processor.Stopping.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.That(run.IsCompleted, Is.False);
            Assert.That(processor.Closed, Is.False);
        }
        finally { finish.TrySetResult(); }
        await run.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(receiver.Outcome, Is.EqualTo(kind));
        Assert.That(receiver.Calls, Is.EqualTo(1));
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task SessionLockLoss_BeforeOrDuringDrain_NeverSettlesSuccessfully(bool duringDrain)
    {
        using var sdkStop = new CancellationTokenSource();
        var processor = new SessionProcessor { SdkStop = sdkStop };
        await using var clients = new AzureServiceBusClientProvider(new Client(processor), new Admin());
        await using var source = Source(clients);
        await source.InitializeAsync(default);
        using var intake = new CancellationTokenSource();
        var receiver = new Receiver { LostLock = true };
        var entered = Signal();
        var finish = Signal();
        bool? accepted = null;
        bool? canceled = null;
        var run = source.RunAsync(async (delivery, token) =>
        {
            await using (delivery)
            {
                entered.SetResult();
                await finish.Task;
                canceled = token.IsCancellationRequested;
                if (!token.IsCancellationRequested) accepted = await delivery.SettleAsync(DispatchResult.Success, token);
            }
        }, intake.Token, default);
        processor.Active = processor.DeliverAsync(Args(receiver, sdkStop.Token));
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        try
        {
            if (duringDrain)
            {
                await intake.CancelAsync();
                await processor.Stopping.Task.WaitAsync(TimeSpan.FromSeconds(5));
            }
            else sdkStop.Cancel();
        }
        finally { finish.TrySetResult(); }
        await processor.Active.WaitAsync(TimeSpan.FromSeconds(5));
        await intake.CancelAsync();
        await run.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(canceled, Is.EqualTo(!duringDrain));
        Assert.That(accepted, duringDrain ? Is.False : Is.Null);
        Assert.That(receiver.Outcome, Is.Null);
        Assert.That(receiver.Calls, Is.EqualTo(duringDrain ? 1 : 0));
        Assert.That(source.Failure.IsCompleted, Is.False);
    }

    [Test]
    public async Task SessionStartupFailure_DisposesPartiallyInitializedProcessor()
    {
        var failure = new IOException("session startup failed");
        var processor = new SessionProcessor { StartFailure = failure };
        await using var clients = new AzureServiceBusClientProvider(new Client(processor), new Admin());
        var descriptor = Descriptor();
        var source = new AzureServiceBusMessagePump(descriptor, clients, new AzureServiceBusTransportOptions(),
            NullLogger<AzureServiceBusMessagePump>.Instance);
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(_ => { });
        await using var provider = services.BuildServiceProvider();
        var lifetime = new PumpLifetime(descriptor, source, provider.GetRequiredService<DeliveryProcessor>());
        Assert.ThrowsAsync<IOException>(() => lifetime.StartAsync(default));
        Assert.ThrowsAsync<IOException>(() => lifetime.Completion);
        Assert.That(processor.Stopping.Task.IsCompletedSuccessfully, Is.True);
        Assert.That(processor.Closed, Is.True);
        Assert.That(lifetime.State, Is.EqualTo(PumpState.Faulted));
        Assert.That(lifetime.Failure, Is.SameAs(failure));
        try { await lifetime.DisposeAsync(); } catch (IOException) { }
    }

    private static AzureServiceBusMessagePump Source(AzureServiceBusClientProvider clients) =>
        new(Descriptor(), clients, new AzureServiceBusTransportOptions(), NullLogger<AzureServiceBusMessagePump>.Instance);
    private static ConsumerDescriptor Descriptor()
    {
        var descriptor = new ConsumerDescriptor(typeof(SessionAdapterTests), new Uri("asbqueue://sessions"));
        new ConsumerBuilder(descriptor, new ServiceCollection()).WithConcurrency(2).WithFifo();
        return descriptor;
    }
    private static ProcessSessionMessageEventArgs Args(Receiver receiver, CancellationToken token) =>
        new(ServiceBusModelFactory.ServiceBusReceivedMessage(new BinaryData("body"), sessionId: "session"), receiver, token);
    private static TaskCompletionSource Signal() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}
