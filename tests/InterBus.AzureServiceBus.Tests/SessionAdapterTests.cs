using Azure.Messaging.ServiceBus;
using InterBus.Consuming;
using InterBus.Dispatching;
using InterBus.Extensions;
using InterBus.MessagePumps;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Azure.Messaging.ServiceBus.Administration;

namespace InterBus.AzureServiceBus.Tests;

[TestFixture]
public class SessionAdapterTests
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
        await Assert.ThrowsAsync<IOException>(() => lifetime.StartAsync(default));
        await Assert.ThrowsAsync<IOException>(() => lifetime.Completion);
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
    private sealed class Admin : ServiceBusAdministrationClient;
    private sealed class Client(SessionProcessor processor) : ServiceBusClient
    {
        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
        public override ServiceBusSessionProcessor CreateSessionProcessor(string queueName, ServiceBusSessionProcessorOptions options)
        {
            Assert.That(options.MaxConcurrentSessions, Is.EqualTo(2));
            Assert.That(options.MaxConcurrentCallsPerSession, Is.EqualTo(1));
            Assert.That(options.AutoCompleteMessages, Is.False);
            Assert.That(options.MaxAutoLockRenewalDuration, Is.EqualTo(TimeSpan.FromHours(12)));
            return processor;
        }
    }
    private sealed class SessionProcessor : ServiceBusSessionProcessor
    {
        public Task Active = Task.CompletedTask;
        public CancellationTokenSource? SdkStop;
        public Exception? StartFailure;
        public bool Closed;
        public TaskCompletionSource Stopping { get; } = Signal();
        private readonly ServiceBusClient _mockClient;
        public SessionProcessor() : this(new ServiceBusClient(
            "Endpoint=sb://unit-test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=dGVzdA==")) { }
        private SessionProcessor(ServiceBusClient client) : base(client, "sessions", new ServiceBusSessionProcessorOptions())
        { _mockClient = client; }
        public Task DeliverAsync(ProcessSessionMessageEventArgs args) => OnProcessSessionMessageAsync(args);
        public override Task StartProcessingAsync(CancellationToken cancellationToken = default) =>
            StartFailure is null ? Task.CompletedTask : Task.FromException(StartFailure);
        public override async Task StopProcessingAsync(CancellationToken cancellationToken = default)
        { SdkStop?.Cancel(); Stopping.TrySetResult(); await Active; }
        public override Task CloseAsync(CancellationToken cancellationToken = default)
        {
            Assert.That(Active.IsCompleted, Is.True);
            Closed = true;
            return _mockClient.DisposeAsync().AsTask();
        }
    }
    private sealed class Receiver : ServiceBusSessionReceiver
    {
        public bool LostLock;
        public int Calls;
        public DispatchResultKind? Outcome;
        public override string SessionId => "session";
        private Task Settle(DispatchResultKind kind)
        {
            Calls++;
            if (LostLock) throw new ServiceBusException("session lost", ServiceBusFailureReason.SessionLockLost);
            Outcome = kind;
            return Task.CompletedTask;
        }
        public override Task CompleteMessageAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken = default) =>
            Settle(DispatchResultKind.Success);
        public override Task AbandonMessageAsync(ServiceBusReceivedMessage message, IDictionary<string, object>? propertiesToModify = null,
            CancellationToken cancellationToken = default) => Settle(DispatchResultKind.Release);
        public override Task DeadLetterMessageAsync(ServiceBusReceivedMessage message, IDictionary<string, object>? propertiesToModify = null,
            string? deadLetterReason = null, string? deadLetterErrorDescription = null, CancellationToken cancellationToken = default) =>
            Settle(DispatchResultKind.DeadLetter);
    }
}
