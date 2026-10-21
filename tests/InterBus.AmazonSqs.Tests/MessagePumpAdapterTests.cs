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
public sealed partial class MessagePumpAdapterTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task BothEnvelopes_DispatchAndDelete_ThroughNewHost(bool serialized)
    {
        using var client = new FakeSqs();
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var envelope = serialized ? (ISnsSqsMessageEnvelope)new SnsSerialisedMessageEnvelope() : new SnsSqsMessageEnvelope();
        client.Messages.Enqueue(new Message
        {
            Body = serialized
            ? "{\"Message\":\"hello\",\"MessageAttributes\":{}}"
            : envelope.SerializeForSnsPublish("hello"u8.ToArray(), new MessageHeaders()),
            ReceiptHandle = "receipt"
        });
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

    [Test]
    public async Task BufferedDeliveries_RenewWhileConsumerIsBlocked()
    {
        using var client = new FakeSqs();
        var envelope = new SnsSqsMessageEnvelope();
        for (var i = 0; i < 4; i++)
            client.Messages.Enqueue(new Message { ReceiptHandle = i.ToString(), Body = envelope.SerializeForSnsPublish([], new MessageHeaders()) });
        var finish = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var provider = Configure(client, async (_, _, _, ct) => await finish.Task.WaitAsync(ct), 1, 3);
        var descriptor = provider.GetRequiredService<InterBusOptions>().Consumers.Single();
        await using var source = (IBufferedSource)provider.GetRequiredService<IConsumerSourceFactory>().Create(descriptor);
        await source.InitializeAsync(default);
        using var intake = new CancellationTokenSource();
        var run = new ConsumerRuntime(descriptor, provider.GetRequiredService<DeliveryProcessor>()).RunAsync(source, intake.Token, default);
        try
        {
            await client.FourRenewed.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.That(client.Deletes, Is.Zero);
        }
        finally { await intake.CancelAsync(); finish.TrySetResult(); }
        await run.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(client.Deletes, Is.EqualTo(4));
    }

    [Test]
    public async Task FifoGroup_IsSerializedThroughDelete_WhileAnotherGroupProgresses()
    {
        using var client = new FakeSqs { BlockDeleteReceipt = "a0" };
        var envelope = new SnsSqsMessageEnvelope();
        foreach (var id in new[] { "a0", "a1", "b0", "b1" })
            client.Messages.Enqueue(new Message
            {
                ReceiptHandle = id,
                Attributes = new Dictionary<string, string> { ["MessageGroupId"] = id[..1] },
                Body = envelope.SerializeForSnsPublish([], new MessageHeaders { ["id"] = id })
            });
        var second = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var independent = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var provider = Configure(client, (ctx, _, _, _) =>
        {
            if (ctx.Headers["id"] == "a1") second.TrySetResult();
            if (ctx.Headers["id"] == "b0") independent.TrySetResult();
            return Task.CompletedTask;
        }, 2, 2);
        var descriptor = provider.GetRequiredService<InterBusOptions>().Consumers.Single();
        await using var source = (IBufferedSource)provider.GetRequiredService<IConsumerSourceFactory>().Create(descriptor);
        await source.InitializeAsync(default);
        using var intake = new CancellationTokenSource();
        var run = new ConsumerRuntime(descriptor, provider.GetRequiredService<DeliveryProcessor>()).RunAsync(source, intake.Token, default);
        try
        {
            await client.DeleteBlocked.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await independent.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.That(second.Task.IsCompleted, Is.False);
        }
        finally { client.AllowDelete.TrySetResult(); await intake.CancelAsync(); }
        await run.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(second.Task.IsCompletedSuccessfully, Is.True);
    }

    private static ServiceProvider Configure(FakeSqs client,
        Func<DispatchContext, ConsumePipelineExecutor, IServiceProvider, CancellationToken, Task> dispatch,
        int concurrency, int prefetch)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IAmazonSQS>(client);
        services.AddSingleton<Amazon.SimpleNotificationService.IAmazonSimpleNotificationService>(
            new Amazon.SimpleNotificationService.AmazonSimpleNotificationServiceClient(new AnonymousAWSCredentials(), RegionEndpoint.USEast1));
        services.AddSingleton<Amazon.SecurityToken.IAmazonSecurityTokenService>(
            new Amazon.SecurityToken.AmazonSecurityTokenServiceClient(new AnonymousAWSCredentials(), RegionEndpoint.USEast1));
        services.AddInterBus(bus =>
        {
            bus.AddAmazonSqsTransport(options => { options.PrefetchCount = prefetch; options.LeaseRenewalInterval = TimeSpan.FromMilliseconds(10); });
            var descriptor = bus.Options.GetConsumerDescriptor(typeof(TestConsumer));
            new ConsumerBuilder<TestConsumer>(descriptor, services).Dispatch(dispatch).WithConcurrency(concurrency).WithPumpCount(3);
        });
        var provider = services.BuildServiceProvider();
        var naming = provider.GetRequiredService<SnsSqsNaming>();
        var queue = provider.GetRequiredService<InterBusOptions>().Consumers.Single().Queue;
        var topology = provider.GetRequiredService<SnsSqsTopologyBuilder>();
        topology.QueueUrls[naming.BuildSqsQueueName(queue)] = "queue";
        topology.QueueUrls[naming.BuildDeadLetterQueueName(queue)] = "dlq";
        return provider;
    }

    [Test]
    public async Task FailedDeadLetterSend_DoesNotDeleteOriginal_AndStopsRenewal()
    {
        using var client = new FakeSqs { FailSend = true };
        var delivery = Delivery(client, new SnsSqsTransportOptions());
        try
        {
            await delivery.SettleAsync(DispatchResult.DeadLetter(DateTimeOffset.UtcNow, new Exception()), default);
            Assert.Fail("Expected send failure");
        }
        catch (IOException) { }
        finally { await delivery.DisposeAsync(); }
        Assert.That(client.Deletes, Is.Zero);
        Assert.That(client.ImmediateReleases, Is.Zero);
    }

    [Test]
    public async Task CancellationAfterReceive_CleansUpTheEntireUntransferredBatch()
    {
        using var intake = new CancellationTokenSource();
        using var client = new FakeSqs { CancelOnReceive = intake };
        var envelope = new SnsSqsMessageEnvelope();
        for (var i = 0; i < 4; i++)
            client.Messages.Enqueue(new Message { ReceiptHandle = i.ToString(), Body = envelope.SerializeForSnsPublish([], new MessageHeaders()) });
        await using var provider = Configure(client, (_, _, _, _) => throw new InvalidOperationException("Must not dispatch"), 2, 2);
        var descriptor = provider.GetRequiredService<InterBusOptions>().Consumers.Single();
        await using var source = (IBufferedSource)provider.GetRequiredService<IConsumerSourceFactory>().Create(descriptor);
        await source.InitializeAsync(default);
        await new ConsumerRuntime(descriptor, provider.GetRequiredService<DeliveryProcessor>())
            .RunAsync(source, intake.Token, default).WaitAsync(TimeSpan.FromSeconds(5));
        await Task.Delay(40);
        Assert.That(client.Deletes, Is.Zero);
        Assert.That(client.Renewals, Is.Zero);
    }

    [Test]
    public async Task FifoDeadLetter_PreservesWireBodyGroupAndDiagnostics()
    {
        using var client = new FakeSqs();
        const string wireBody = "{invalid-original-wire-data}";
        await using var delivery = new SqsPrefetchedMessage(NullLogger<SqsPrefetchedMessage>.Instance,
            client, new SnsSqsTransportOptions(), new Message
            {
                Body = wireBody,
                ReceiptHandle = "receipt",
                Attributes = new Dictionary<string, string> { ["MessageGroupId"] = "group" }
            },
            "queue.fifo", "dlq.fifo", new SnsSqsMessageEnvelope());
        var result = DispatchResult.DeadLetter(DateTimeOffset.UtcNow, new FormatException("decode"));
        Assert.That(await delivery.SettleAsync(result, default), Is.True);
        Assert.That(client.LastSend!.MessageBody, Is.EqualTo(wireBody));
        Assert.That(client.LastSend.MessageGroupId, Is.EqualTo("group"));
        Assert.That(client.LastSend.MessageDeduplicationId, Is.Not.Null.And.Not.Empty);
        Assert.That(client.LastSend.MessageAttributes[InterBusHeaders.DeadLetterReason].StringValue,
            Is.EqualTo(nameof(FormatException)));
        Assert.That(client.Deletes, Is.EqualTo(1));
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
}
