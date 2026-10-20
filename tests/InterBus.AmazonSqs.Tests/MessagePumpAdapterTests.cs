using System.Collections.Concurrent;
using Amazon;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using InterBus.AmazonSqs.Extensions;
using InterBus.AmazonSqs.MessagePumps;
using InterBus.Consuming;
using InterBus.Dispatching;
using InterBus.Extensions;
using InterBus.MessagePumps;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;

namespace InterBus.AmazonSqs.Tests;

[TestFixture]
public sealed class MessagePumpAdapterTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task BothEnvelopes_DispatchAndDelete_ThroughNewHost(bool serialized)
    {
        using var client = new FakeSqs();
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var envelope = serialized ? (ISnsSqsMessageEnvelope)new SnsSerialisedMessageEnvelope() : new SnsSqsMessageEnvelope();
        client.Messages.Enqueue(new Message { Body = serialized
            ? "{\"Message\":\"hello\",\"MessageAttributes\":{}}"
            : envelope.SerializeForSnsPublish("hello"u8.ToArray(), new MessageHeaders()), ReceiptHandle = "receipt" });
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton<IAmazonSQS>(client);
        builder.Services.AddSingleton<Amazon.SimpleNotificationService.IAmazonSimpleNotificationService>(
            new Amazon.SimpleNotificationService.AmazonSimpleNotificationServiceClient(new AnonymousAWSCredentials(), RegionEndpoint.USEast1));
        builder.Services.AddSingleton<Amazon.SecurityToken.IAmazonSecurityTokenService>(
            new Amazon.SecurityToken.AmazonSecurityTokenServiceClient(new AnonymousAWSCredentials(), RegionEndpoint.USEast1));
        builder.Services.AddInterBus(bus =>
        {
            bus.AddAmazonSqsTransport();
            var descriptor = bus.Options.GetConsumerDescriptor(typeof(TestConsumer));
            new ConsumerBuilder<TestConsumer>(descriptor, bus.Services).Dispatch((ctx, _, _, _) =>
            {
                Assert.That(ctx.Payload, Is.EqualTo("hello"u8.ToArray()));
                received.SetResult();
                return Task.CompletedTask;
            }).WithConcurrency(2).WithPumpCount(3);
            bus.Services.AddSingleton<ISnsSqsMessageEnvelope>(envelope);
        });
        // Supply deterministic topology while retaining the production adapter and host registrations
        builder.Services.RemoveAll<ITopologyBuilder>();
        builder.Services.AddSingleton<ITopologyBuilder, CachedTopology>();
        using var host = builder.Build();
        var topology = host.Services.GetRequiredService<SnsSqsTopologyBuilder>();
        var naming = host.Services.GetRequiredService<SnsSqsNaming>();
        var descriptor = host.Services.GetRequiredService<InterBusOptions>().Consumers.Single();
        topology.QueueUrls[naming.BuildSqsQueueName(descriptor.Queue)] = "queue";
        topology.QueueUrls[naming.BuildDeadLetterQueueName(descriptor.Queue)] = "dlq";
        await host.StartAsync();
        await received.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await client.Deleted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(host.Services.GetRequiredService<IConsumerSourceFactory>(), Is.TypeOf<SqsQueueMessagePumpFactory>());
        await host.StopAsync();
        Assert.That(client.Deletes, Is.EqualTo(1));
    }

    [TestCase(DispatchResultKind.Success)]
    [TestCase(DispatchResultKind.DeadLetter)]
    public async Task SettlementFailure_StopsRenewal_AndDoesNotRelease(DispatchResultKind kind)
    {
        using var client = new FakeSqs { FailDelete = true };
        var options = new SnsSqsTransportOptions { LeaseRenewalInterval = TimeSpan.FromMilliseconds(10) };
        var delivery = Delivery(client, options);
        await client.Renewed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var result = kind == DispatchResultKind.Success ? DispatchResult.Success
            : DispatchResult.DeadLetter(DateTimeOffset.UtcNow, new Exception("failed"));
        try { await delivery.SettleAsync(result, default); Assert.Fail("Expected delete failure"); }
        catch (IOException) { }
        await delivery.DisposeAsync();
        var renewals = client.Renewals;
        await Task.Delay(40);
        Assert.That(client.Renewals, Is.EqualTo(renewals));
        Assert.That(client.ImmediateReleases, Is.Zero);
        Assert.That(client.Deletes, Is.EqualTo(1));
    }

    [Test]
    public async Task Release_PreservesVisibilityTimeout()
    {
        using var client = new FakeSqs();
        await using var delivery = Delivery(client, new SnsSqsTransportOptions());
        Assert.That(await delivery.SettleAsync(DispatchResult.Release(new Exception()), default), Is.True);
        Assert.That(client.ImmediateReleases, Is.Zero);
        Assert.That(client.Deletes, Is.Zero);
    }

    [Test]
    public async Task MalformedEnvelope_RetainsOwnershipForCleanup()
    {
        using var client = new FakeSqs();
        await using var delivery = new SqsPrefetchedMessage(NullLogger<SqsPrefetchedMessage>.Instance, client,
            new SnsSqsTransportOptions(), new Message { Body = "invalid", ReceiptHandle = "receipt" },
            "queue", "dlq", new SnsSqsMessageEnvelope());
        Assert.That(delivery.DecodeError, Is.Not.Null);
        Assert.That(await delivery.SettleAsync(DispatchResult.Release(delivery.DecodeError!), default), Is.True);
    }

    [Test]
    public async Task FailedRenewal_RejectsSettlement()
    {
        using var client = new FakeSqs { FailRenewal = true };
        var delivery = Delivery(client, new SnsSqsTransportOptions
        { LeaseRenewalInterval = TimeSpan.FromMilliseconds(1) });
        await client.Renewed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        // Completion of the renewal loop is observed by disposal before checking rejected settlement
        await delivery.DisposeAsync();
        Assert.That(await delivery.SettleAsync(DispatchResult.Success, default), Is.False);
        Assert.That(client.Deletes, Is.Zero);
    }

    [TestCase(-1, 20)]
    [TestCase(0, 21)]
    public void InvalidOptions_AreRejected(int prefetch, int wait)
    {
        var options = new SnsSqsTransportOptions { PrefetchCount = prefetch, ReceiveWaitTimeSeconds = wait };
        Assert.Throws<ArgumentOutOfRangeException>(options.Validate);
    }

    private static SqsPrefetchedMessage Delivery(FakeSqs client, SnsSqsTransportOptions options) =>
        new(NullLogger<SqsPrefetchedMessage>.Instance, client, options,
            new Message { Body = new SnsSqsMessageEnvelope().SerializeForSnsPublish([], new MessageHeaders()), ReceiptHandle = "receipt" },
            "queue", "dlq", new SnsSqsMessageEnvelope());

    private sealed class TestConsumer : IConsumer;
    private sealed class CachedTopology : ITopologyBuilder
    {
        public Task BuildConsumerTopologyAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
    private sealed class FakeSqs() : AmazonSQSClient(new AnonymousAWSCredentials(), RegionEndpoint.USEast1)
    {
        public ConcurrentQueue<Message> Messages { get; } = new();
        public bool FailDelete, FailRenewal;
        public int Deletes, Renewals, ImmediateReleases;
        public TaskCompletionSource Deleted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Renewed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public override async Task<ReceiveMessageResponse> ReceiveMessageAsync(ReceiveMessageRequest request, CancellationToken cancellationToken = default)
        {
            if (Messages.TryDequeue(out var message)) return new ReceiveMessageResponse { Messages = [message] };
            await Task.Delay(Timeout.Infinite, cancellationToken);
            return new ReceiveMessageResponse();
        }
        public override Task<DeleteMessageResponse> DeleteMessageAsync(DeleteMessageRequest request, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref Deletes);
            if (FailDelete) throw new IOException("delete failed");
            Deleted.TrySetResult();
            return Task.FromResult(new DeleteMessageResponse());
        }
        public override Task<ChangeMessageVisibilityResponse> ChangeMessageVisibilityAsync(ChangeMessageVisibilityRequest request, CancellationToken cancellationToken = default)
        {
            if (request.VisibilityTimeout == 0) Interlocked.Increment(ref ImmediateReleases);
            Interlocked.Increment(ref Renewals);
            Renewed.TrySetResult();
            if (FailRenewal) throw new ReceiptHandleIsInvalidException("lost");
            return Task.FromResult(new ChangeMessageVisibilityResponse());
        }
        public override Task<SendMessageResponse> SendMessageAsync(SendMessageRequest request, CancellationToken cancellationToken = default) =>
            Task.FromResult(new SendMessageResponse());
    }
}
