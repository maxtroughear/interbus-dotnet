using System.Globalization;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using InterBus.AzureServiceBus.Extensions;
using InterBus.Extensions;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.AzureServiceBus.Tests;

[TestFixture]
public class SchedulingProviderTests
{
    [TestCase(long.MinValue)]
    [TestCase(long.MaxValue)]
    [TestCase(0L)]
    public void IdRoundTripsAcrossCultures(long sequence)
    {
        var original = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("ar-SA");
            var id = AzureServiceBusScheduleId.Encode("prefix/topic:part/日本", sequence);
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("fr-FR");
            Assert.That(AzureServiceBusScheduleId.Decode(id), Is.EqualTo(("prefix/topic:part/日本", sequence)));
        }
        finally { CultureInfo.CurrentCulture = original; }
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("asb:v2:cXVldWU:1")]
    [TestCase("asb:v1::1")]
    [TestCase("asb:v1:***:1")]
    [TestCase("asb:v1:cXVldWU:9223372036854775808")]
    [TestCase("asb:v1:cXVldWU:1:extra")]
    public void InvalidIdsAreRejected(string? id) =>
        Assert.Throws<ArgumentException>(() => AzureServiceBusScheduleId.Decode(id!));

    [TestCase(false, false)]
    [TestCase(false, true)]
    [TestCase(true, false)]
    [TestCase(true, true)]
    public async Task ScheduleAndCancelPreserveDestinationAndMessage(bool topic, bool alreadyDue)
    {
        var sender = new Sender();
        var client = new Client(sender);
        await using var clients = new AzureServiceBusClientProvider(client, new Admin());
        var topology = new Topology();
        var options = new AzureServiceBusTransportOptions { QueuePrefix = "prefix+" };
        var provider = new AzureServiceBusSchedulingProvider(clients, options, topology);
        var destination = new Uri(topic ? "azureservicebustopic://interbus/orders" : "azureservicebusqueue://interbus/orders?vq=reply");
        var headers = new MessageHeaders { [InterBusHeaders.MessageGroupId] = "group", ["custom"] = "value" };
        var time = DateTimeOffset.UtcNow.AddMinutes(alreadyDue ? -1 : 5);
        using var cts = new CancellationTokenSource();
        var id = await provider.ScheduleAsync(destination, [1, 2, 3], headers, time, cts.Token);
        var expected = topic ? "physical/topic" : "prefix-orders";
        Assert.Multiple(() =>
        {
            Assert.That(client.Path, Is.EqualTo(expected));
            Assert.That(sender.Time, Is.EqualTo(time));
            Assert.That(sender.Token, Is.EqualTo(cts.Token));
            Assert.That(sender.Message!.Body.ToArray(), Is.EqualTo(new byte[] { 1, 2, 3 }));
            Assert.That(sender.Message.ApplicationProperties["custom"], Is.EqualTo("value"));
            Assert.That(sender.Message.SessionId, Is.EqualTo(topic ? "group" : "reply"));
            Assert.That(topology.Destination, Is.EqualTo(topic ? destination : null));
        });
        options.QueuePrefix = "changed-";
        var freshClient = new Client(sender);
        await using var freshClients = new AzureServiceBusClientProvider(freshClient, new Admin());
        await new AzureServiceBusSchedulingProvider(freshClients, options, topology).CancelAsync(id, cts.Token);
        Assert.That(freshClient.Path, Is.EqualTo(expected));
        Assert.That(sender.Canceled, Is.EqualTo(long.MaxValue));
        Assert.That(sender.Token, Is.EqualTo(cts.Token));
    }

    [Test]
    public async Task InvalidInputDoesNotCreateSenderAndSdkFailuresPropagate()
    {
        var sender = new Sender { Failure = new IOException("broker failure") };
        var client = new Client(sender);
        await using var clients = new AzureServiceBusClientProvider(client, new Admin());
        var provider = new AzureServiceBusSchedulingProvider(clients, new(), new Topology());
        await Assert.ThrowsAsync<ArgumentException>(() => provider.ScheduleAsync(new Uri("memoryqueue://interbus/test"), [], new(), DateTimeOffset.UtcNow, default));
        Assert.Throws<ArgumentException>(() => provider.CancelAsync("bad", default));
        Assert.That(client.Path, Is.Null);
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAsync<OperationCanceledException>(() => provider.ScheduleAsync(
            new Uri("azureservicebusqueue://interbus/test"), [], new(), DateTimeOffset.UtcNow, cts.Token));
        await Assert.ThrowsAsync<OperationCanceledException>(() => provider.CancelAsync(
            AzureServiceBusScheduleId.Encode("test", 1), cts.Token));

        await Assert.ThrowsAsync<IOException>(() => provider.ScheduleAsync(new Uri("azureservicebusqueue://interbus/test"), [], new(), DateTimeOffset.UtcNow, default));
        await Assert.ThrowsAsync<IOException>(() => provider.CancelAsync(AzureServiceBusScheduleId.Encode("test", 1), default));
    }

    [Test]
    public async Task RegistrationResolvesSchedulerAndSingletonProvider()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus =>
        {
            bus.AddAzureServiceBusTransport(o => o.ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=dGVzdA==");
            bus.AddScheduling(s => s.AddAzureServiceBusScheduler().Directly());
        });
        await using var servicesProvider = services.BuildServiceProvider();
        using var scope = servicesProvider.CreateScope();
        Assert.That(scope.ServiceProvider.GetRequiredService<IScheduler>(), Is.Not.Null);
        Assert.That(scope.ServiceProvider.GetRequiredService<IMessageSchedulingProvider>(), Is.TypeOf<AzureServiceBusSchedulingProvider>());
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task TransportDelayAndOrdinarySendPreserveMessage(bool delayed)
    {
        var sender = new Sender();
        await using var clients = new AzureServiceBusClientProvider(new Client(sender), new Admin());
        var transport = new AzureServiceBusQueueTransport(clients, new());
        var before = DateTimeOffset.UtcNow;
        await transport.SendAsync(new InterBus.Transporting.TransportSendContext(
            new Uri("azureservicebusqueue://interbus/orders?vq=reply"), [4, 5],
            new MessageHeaders { ["custom"] = "value" }, delayed ? TimeSpan.FromMinutes(1) : null), default);
        Assert.That(sender.Message!.SessionId, Is.EqualTo("reply"));
        Assert.That(sender.Message.ApplicationProperties["custom"], Is.EqualTo("value"));
        if (delayed)
            Assert.That(sender.Message.ScheduledEnqueueTime, Is.InRange(before.AddMinutes(1), DateTimeOffset.UtcNow.AddMinutes(1)));
        else Assert.That(sender.Message.ScheduledEnqueueTime, Is.EqualTo(default(DateTimeOffset)));
    }

    [Test]
    public async Task NamespaceCredentialConfigurationConstructsBothClients()
    {
        await using var clients = new AzureServiceBusClientProvider(new AzureServiceBusTransportOptions
        {
            FullyQualifiedNamespace = "test.servicebus.windows.net",
            Credential = new Azure.Identity.AzureCliCredential()
        });
        Assert.That(clients.AdminClient, Is.Not.Null);
        Assert.That(clients.GetOrCreateSender("orders").FullyQualifiedNamespace, Is.EqualTo("test.servicebus.windows.net"));
    }

    [Test]
    public void NamespaceWithoutCredentialIsRejected() => Assert.Throws<ArgumentException>(() =>
        new AzureServiceBusClientProvider(new AzureServiceBusTransportOptions
        {
            FullyQualifiedNamespace = "test.servicebus.windows.net"
        }));

    private sealed class Admin : ServiceBusAdministrationClient;
    private sealed class Client(Sender sender) : ServiceBusClient
    {
        public string? Path;
        public override ServiceBusSender CreateSender(string queueOrTopicName) { Path = queueOrTopicName; return sender; }
        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
    private sealed class Sender : ServiceBusSender
    {
        public ServiceBusMessage? Message;
        public DateTimeOffset Time;
        public CancellationToken Token;
        public long Canceled;
        public Exception? Failure;
        public override Task SendMessageAsync(ServiceBusMessage message, CancellationToken cancellationToken = default)
        { Message = message; Token = cancellationToken; return Task.CompletedTask; }
        public override Task<long> ScheduleMessageAsync(ServiceBusMessage message, DateTimeOffset scheduledEnqueueTime, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (Failure is not null) throw Failure;
            Message = message; Time = scheduledEnqueueTime; Token = cancellationToken;
            return Task.FromResult(long.MaxValue);
        }
        public override Task CancelScheduledMessageAsync(long sequenceNumber, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (Failure is not null) throw Failure;
            Canceled = sequenceNumber; Token = cancellationToken;
            return Task.CompletedTask;
        }
        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
    private sealed class Topology : ITopologyBuilder
    {
        public Uri? Destination;
        public Task BuildConsumerTopologyAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<string> EnsureTopicAsync(Uri topic, CancellationToken cancellationToken)
        { Destination = topic; return Task.FromResult("physical/topic"); }
    }
}
