using Azure.Messaging.ServiceBus;
using InterBus.Consuming;
using InterBus.Dispatching;
using Microsoft.Extensions.Logging.Abstractions;

namespace InterBus.AzureServiceBus.Tests;

[TestFixture]
public partial class ManagedAdapterTests
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
        var run = source.RunAsync(async (delivery, ct) =>
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
        var run = source.RunAsync(async (delivery, ct) =>
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
        var run = source.RunAsync((_, _) => Task.CompletedTask, intake.Token, default);
        await processor.ReportAsync(new ProcessErrorEventArgs(new ServiceBusException("temporary",
            ServiceBusFailureReason.ServiceCommunicationProblem), ServiceBusErrorSource.Receive,
            "namespace", "queue", "processor", default));
        Assert.That(run.IsCompleted, Is.False);
        Assert.That(source.Failure.IsCompleted, Is.False);
        await intake.CancelAsync();
        await run;
    }
}
