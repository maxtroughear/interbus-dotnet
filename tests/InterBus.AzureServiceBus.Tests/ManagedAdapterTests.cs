using Azure.Messaging.ServiceBus;
using InterBus.Consuming;
using InterBus.Dispatching;
using Microsoft.Extensions.Logging.Abstractions;
using Azure.Messaging.ServiceBus.Administration;
using InterBus.MessagePumps;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.AzureServiceBus.Tests;

[TestFixture]
public class ManagedAdapterTests
{
    [TestCase(DispatchResultKind.Success)]
    [TestCase(DispatchResultKind.Release)]
    [TestCase(DispatchResultKind.DeadLetter)]
    public async Task Callback_AwaitsSettlement_BeforeStopAndDisposal(DispatchResultKind kind)
    {
        using var sdkStop = new CancellationTokenSource();
        var processor = new FakeProcessor { SdkStop = sdkStop };
        var client = new FakeClient(processor);
        await using var provider = new AzureServiceBusClientProvider(client, new FakeAdmin());
        var descriptor = new ConsumerDescriptor(typeof(ManagedAdapterTests), new Uri("asbqueue://test"));
        await using var source = new AzureServiceBusMessagePump(descriptor, provider,
            new AzureServiceBusTransportOptions(), NullLogger<AzureServiceBusMessagePump>.Instance);
        await source.InitializeAsync(default);
        using var intake = new CancellationTokenSource();
        var receiver = new FakeReceiver();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var finish = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var run = RunAsync(source, async (delivery, ct) =>
        {
            entered.SetResult();
            await finish.Task.WaitAsync(ct);
            await using (delivery)
            {
                var result = kind switch
                {
                    DispatchResultKind.Success => DispatchResult.Success,
                    DispatchResultKind.Release => DispatchResult.Release(new Exception()),
                    _ => DispatchResult.DeadLetter(DateTimeOffset.UtcNow, new Exception())
                };
                await delivery.SettleAsync(result, ct);
            }
        }, intake.Token, default);
        processor.Active = processor.DeliverAsync(new ProcessMessageEventArgs(
            ServiceBusModelFactory.ServiceBusReceivedMessage(new BinaryData("body")), receiver, sdkStop.Token));
        await entered.Task;
        await intake.CancelAsync();
        Assert.That(run.IsCompleted, Is.False);
        finish.SetResult();
        await processor.Active;
        await run.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(receiver.Outcome, Is.EqualTo(kind));
        Assert.That(processor.Stopped, Is.True);
    }

    [Test]
    public async Task LockLoss_IsRejected_WithoutConflictingAbandon()
    {
        var processor = new FakeProcessor();
        await using var provider = new AzureServiceBusClientProvider(new FakeClient(processor), new FakeAdmin());
        await using var source = new AzureServiceBusMessagePump(
            new ConsumerDescriptor(typeof(ManagedAdapterTests), new Uri("asbqueue://test")), provider,
            new AzureServiceBusTransportOptions(), NullLogger<AzureServiceBusMessagePump>.Instance);
        await source.InitializeAsync(default);
        using var intake = new CancellationTokenSource();
        bool? accepted = null;
        var run = RunAsync(source, async (delivery, ct) =>
        {
            await using (delivery) accepted = await delivery.SettleAsync(DispatchResult.Success, ct);
        }, intake.Token, default);
        var receiver = new FakeReceiver { LoseLock = true };
        await processor.DeliverAsync(new ProcessMessageEventArgs(
            ServiceBusModelFactory.ServiceBusReceivedMessage(new BinaryData("body")), receiver, default));
        await intake.CancelAsync();
        await run;
        Assert.That(accepted, Is.False);
        Assert.That(receiver.Outcome, Is.Null);
    }

    [Test]
    public async Task RecoverableSdkError_DoesNotFaultTheConsumerRuntime()
    {
        var processor = new FakeProcessor();
        await using var provider = new AzureServiceBusClientProvider(new FakeClient(processor), new FakeAdmin());
        await using var source = new AzureServiceBusMessagePump(
            new ConsumerDescriptor(typeof(ManagedAdapterTests), new Uri("asbqueue://test")), provider,
            new AzureServiceBusTransportOptions(), NullLogger<AzureServiceBusMessagePump>.Instance);
        await source.InitializeAsync(default);
        using var intake = new CancellationTokenSource();
        var run = RunAsync(source, (_, _) => Task.CompletedTask, intake.Token, default);
        await processor.ReportAsync(new ProcessErrorEventArgs(new ServiceBusException("temporary",
            ServiceBusFailureReason.ServiceCommunicationProblem), ServiceBusErrorSource.Receive,
            "namespace", "queue", "processor", default));
        Assert.That(run.IsCompleted, Is.False);
        Assert.That(source.Failure.IsCompleted, Is.False);
        await intake.CancelAsync();
        await run;
    }

    private sealed class FakeAdmin : ServiceBusAdministrationClient;
    private sealed class FakeClient(FakeProcessor processor) : ServiceBusClient
    {
        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
        public override ServiceBusProcessor CreateProcessor(string queueName, ServiceBusProcessorOptions options)
        {
            Assert.That(options.AutoCompleteMessages, Is.False);
            return processor;
        }
    }
    private sealed class FakeProcessor : ServiceBusProcessor
    {
        public int Starts;
        public Task Active = Task.CompletedTask;
        public bool Stopped;
        public CancellationTokenSource? SdkStop;
        public Task DeliverAsync(ProcessMessageEventArgs args) => OnProcessMessageAsync(args);
        public Task ReportAsync(ProcessErrorEventArgs args) => OnProcessErrorAsync(args);
        public override Task StartProcessingAsync(CancellationToken cancellationToken = default) { Starts++; return Task.CompletedTask; }
        public override async Task StopProcessingAsync(CancellationToken cancellationToken = default)
        { SdkStop?.Cancel(); await Active; Stopped = true; }
        public override Task CloseAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
    }
    private sealed class FakeReceiver : ServiceBusReceiver
    {
        public bool LoseLock;
        public DispatchResultKind? Outcome;
        public override Task CompleteMessageAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken = default)
        {
            if (LoseLock) throw new ServiceBusException("lost", ServiceBusFailureReason.MessageLockLost);
            Outcome = DispatchResultKind.Success; return Task.CompletedTask;
        }
        public override Task AbandonMessageAsync(ServiceBusReceivedMessage message,
            IDictionary<string, object>? propertiesToModify = null, CancellationToken cancellationToken = default)
        { Outcome = DispatchResultKind.Release; return Task.CompletedTask; }
        public override Task DeadLetterMessageAsync(ServiceBusReceivedMessage message,
            IDictionary<string, object>? propertiesToModify = null, string? deadLetterReason = null,
            string? deadLetterErrorDescription = null, CancellationToken cancellationToken = default)
        { Outcome = DispatchResultKind.DeadLetter; return Task.CompletedTask; }
    }
    [Test]
    public async Task RepeatedPauseResume_RetainsOneCallbackRegistration()
    {
        var processor = new FakeProcessor();
        await using var clients = new AzureServiceBusClientProvider(new FakeClient(processor), new FakeAdmin());
        await using var source = new AzureServiceBusMessagePump(new ConsumerDescriptor(typeof(ManagedAdapterTests), new Uri("asbqueue://test")), clients, new AzureServiceBusTransportOptions(), NullLogger<AzureServiceBusMessagePump>.Instance);
        await source.InitializeAsync(default);
        var calls = 0;
        await source.StartAsync(async (delivery, token) =>
        {
            Interlocked.Increment(ref calls);
            await using (delivery) await delivery.SettleAsync(DispatchResult.Success, token);
        }, default, default);
        for (var i = 0; i < 3; i++)
        {
            await source.PauseAsync(default);
            await source.ResumeAsync(default);
            await processor.DeliverAsync(new ProcessMessageEventArgs(ServiceBusModelFactory.ServiceBusReceivedMessage(new BinaryData("body")), new FakeReceiver(), default));
        }
        await source.PauseAsync(default);
        Assert.That(calls, Is.EqualTo(3));
        Assert.That(processor.Starts, Is.EqualTo(4));
    }

    private static async Task RunAsync(ICallbackSource source, DeliveryHandler process, CancellationToken intakeToken,
        CancellationToken processingToken)
    {
        try
        {
            await source.StartAsync(process, processingToken, intakeToken);
            await Task.Delay(Timeout.Infinite, intakeToken);
        }
        catch (OperationCanceledException) when (intakeToken.IsCancellationRequested) { }
        finally { await source.PauseAsync(CancellationToken.None); }
    }

}
