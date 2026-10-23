using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using InterBus.Consuming;
using InterBus.Dispatching;
using Microsoft.Extensions.Logging.Abstractions;

namespace InterBus.AzureServiceBus.Tests;

[TestFixture]
public class ProbeAdapterTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task NormalProbe_ReceivesOnceWithoutPrefetch_AndDisposesAfterSettlement(bool empty)
    {
        var receiver = new Receiver { Empty = empty };
        var client = new Client(receiver);
        await using var clients = new AzureServiceBusClientProvider(client, new Admin());
        await using var source = Source(clients);
        var dispatched = 0;
        await source.InitializeAsync(default);
        await source.StartAsync(async (delivery, token) =>
        {
            Assert.That(receiver.Closed, Is.False);
            dispatched++;
            await using (delivery) Assert.That(await delivery.SettleAsync(DispatchResult.Success, token), Is.True);
        }, default, default);
        await source.PauseAsync(default);
        var result = await source.ReceiveProbeAsync(default, TestContext.CurrentContext.CancellationToken);
        Assert.That(result.FoundWork, Is.EqualTo(!empty));
        Assert.That(dispatched, Is.EqualTo(empty ? 0 : 1));
        Assert.That(receiver.Receives, Is.EqualTo(1));
        Assert.That(receiver.Closed, Is.True);
        Assert.That(client.Processor.Starts, Is.EqualTo(1));
        await source.ResumeAsync(default);
        Assert.That(client.Processor.Starts, Is.EqualTo(2));
    }

    [Test]
    public async Task ProbeRenewalLoss_PreventsSuccess_WithoutConflictingSettlement()
    {
        var receiver = new Receiver { LoseRenewal = true };
        await using var clients = new AzureServiceBusClientProvider(new Client(receiver), new Admin());
        await using var source = Source(clients);
        await source.InitializeAsync(default);
        await source.StartAsync(async (delivery, token) =>
        {
            await using (delivery)
            {
                await receiver.Renewed.Task.WaitAsync(TimeSpan.FromSeconds(5), token);
                Assert.That(await delivery.SettleAsync(DispatchResult.Success, token), Is.False);
            }
        }, TestContext.CurrentContext.CancellationToken, default);
        await source.PauseAsync(default);
        var error = await Assert.ThrowsAsync<ServiceBusException>(async () =>
            await source.ReceiveProbeAsync(TestContext.CurrentContext.CancellationToken, TestContext.CurrentContext.CancellationToken));
        Assert.That(error!.Reason, Is.EqualTo(ServiceBusFailureReason.MessageLockLost));
        Assert.That(source.IsRecoverable(error), Is.True);
        Assert.That(receiver.Settlements, Is.LessThanOrEqualTo(1));
        Assert.That(receiver.Closed, Is.True);
    }

    private static AzureServiceBusMessagePump Source(AzureServiceBusClientProvider clients) => new(
        new ConsumerDescriptor(typeof(ProbeAdapterTests), new Uri("asbqueue://test")), clients,
        new AzureServiceBusTransportOptions(), NullLogger<AzureServiceBusMessagePump>.Instance);
    private sealed class Admin : ServiceBusAdministrationClient;
    private sealed class Client(Receiver receiver) : ServiceBusClient
    {
        public Processor Processor { get; } = new();
        public override ServiceBusProcessor CreateProcessor(string queueName, ServiceBusProcessorOptions options) => Processor;
        public override ServiceBusReceiver CreateReceiver(string queueName, ServiceBusReceiverOptions options)
        {
            Assert.That(options.PrefetchCount, Is.Zero);
            Assert.That(options.ReceiveMode, Is.EqualTo(ServiceBusReceiveMode.PeekLock));
            return receiver;
        }
        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
    private sealed class Processor : ServiceBusProcessor
    {
        public int Starts;
        public override Task StartProcessingAsync(CancellationToken cancellationToken = default) { Starts++; return Task.CompletedTask; }
        public override Task StopProcessingAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public override Task CloseAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
    }
    private sealed class Receiver : ServiceBusReceiver
    {
        public bool Empty;
        public bool LoseRenewal;
        public bool Closed;
        public int Receives;
        public int Settlements;
        public TaskCompletionSource Renewed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public override Task<ServiceBusReceivedMessage> ReceiveMessageAsync(TimeSpan? maxWaitTime = null, CancellationToken cancellationToken = default)
        {
            Receives++;
            return Task.FromResult(Empty ? null! : ServiceBusModelFactory.ServiceBusReceivedMessage(new BinaryData("message"),
                lockedUntil: DateTimeOffset.UtcNow + (LoseRenewal ? TimeSpan.FromMilliseconds(200) : TimeSpan.FromMinutes(1))));
        }
        public override Task RenewMessageLockAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken = default)
        {
            Renewed.TrySetResult();
            return Task.FromException(new ServiceBusException("renewal lost", ServiceBusFailureReason.MessageLockLost));
        }
        public override Task CompleteMessageAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken = default)
        {
            Settlements++;
            if (LoseRenewal) throw new ServiceBusException("lock lost", ServiceBusFailureReason.MessageLockLost);
            return Task.CompletedTask;
        }
        public override Task CloseAsync(CancellationToken cancellationToken = default) { Closed = true; return Task.CompletedTask; }
    }
}
