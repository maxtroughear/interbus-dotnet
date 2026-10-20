using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using InterBus.Consuming;
using InterBus.Dispatching;
using InterBus.MessagePumps;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace InterBus.AzureServiceBus.Tests;

[TestFixture]
public class ManagedAdapterTests
{
    [TestCase(DispatchResultKind.Success)]
    [TestCase(DispatchResultKind.Release)]
    [TestCase(DispatchResultKind.DeadLetter)]
    public async Task Callback_AwaitsSettlement_BeforeStopAndDisposal(DispatchResultKind kind)
    {
        var processor = new FakeProcessor();
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
            await finish.Task;
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
            ServiceBusModelFactory.ServiceBusReceivedMessage(new BinaryData("body")), receiver, default));
        await entered.Task;
        await intake.CancelAsync();
        Assert.That(run.IsCompleted, Is.False);
        finish.SetResult();
        await processor.Active;
        await run.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(receiver.Outcome, Is.EqualTo(kind));
        Assert.That(processor.Stopped, Is.True);
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
        public Task Active = Task.CompletedTask;
        public bool Stopped;
        public Task DeliverAsync(ProcessMessageEventArgs args) => OnProcessMessageAsync(args);
        public override Task StartProcessingAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public override async Task StopProcessingAsync(CancellationToken cancellationToken = default)
        { await Active; Stopped = true; }
        public override Task CloseAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
    }
    private sealed class FakeReceiver : ServiceBusReceiver
    {
        public DispatchResultKind? Outcome;
        public override Task CompleteMessageAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken = default)
        { Outcome = DispatchResultKind.Success; return Task.CompletedTask; }
        public override Task AbandonMessageAsync(ServiceBusReceivedMessage message,
            IDictionary<string, object>? propertiesToModify = null, CancellationToken cancellationToken = default)
        { Outcome = DispatchResultKind.Release; return Task.CompletedTask; }
        public override Task DeadLetterMessageAsync(ServiceBusReceivedMessage message,
            IDictionary<string, object>? propertiesToModify = null, string? deadLetterReason = null,
            string? deadLetterErrorDescription = null, CancellationToken cancellationToken = default)
        { Outcome = DispatchResultKind.DeadLetter; return Task.CompletedTask; }
    }
}
