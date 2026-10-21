using Amazon.SQS.Model;
using InterBus.AmazonSqs.MessagePumps;
using InterBus.Consuming;
using InterBus.Dispatching;
using InterBus.Extensions;
using InterBus.MessagePumps;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.AmazonSqs.Tests;

[TestFixture]
public partial class SettlementOwnershipRaceTests
{
    [TestCase("ack")]
    [TestCase("dead-letter-send")]
    [TestCase("dead-letter-delete")]
    public async Task RenewalLoss_DuringSettlement_IsRejectedWithoutConflictingSettlement(string blockedOperation)
    {
        using var client = new ControlledSqs(blockedOperation);
        var log = new CaptureLog();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(_ => { });
        await using var provider = services.BuildServiceProvider();
        var descriptor = new ConsumerDescriptor(typeof(SettlementOwnershipRaceTests), new Uri("sqsqueue://race"));
        var result = blockedOperation == "ack" ? DispatchResult.Success :
            DispatchResult.DeadLetter(DateTimeOffset.UtcNow, new IOException("consumer failure"));
        new ConsumerBuilder<RaceConsumer>(descriptor, services).Dispatch((context, _, _, _) =>
        { context.Result = result; return Task.CompletedTask; });
        var envelope = new SnsSqsMessageEnvelope();
        var delivery = new SqsPrefetchedMessage(log, client, new SnsSqsTransportOptions
        { LeaseRenewalInterval = TimeSpan.FromMilliseconds(10) },
            new Message { Body = envelope.SerializeForSnsPublish([], new MessageHeaders()), ReceiptHandle = "receipt" },
            "queue", "dlq", envelope);
        var process = new DeliveryProcessor(provider.GetRequiredService<Dispatcher>(), log)
            .ProcessAsync(descriptor, delivery, TestContext.CurrentContext.CancellationToken);
        try
        {
            await client.OperationEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await client.RenewalEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));
            client.FailRenewal.TrySetResult();
            await log.OwnershipLost.Task.WaitAsync(TimeSpan.FromSeconds(5));
        }
        finally { client.AllowOperation.TrySetResult(); }
        await process.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(client.Deletes, Is.EqualTo(blockedOperation == "dead-letter-send" ? 0 : 1));
        Assert.That(client.ImmediateReleases, Is.Zero);
        Assert.That(log.Messages.Any(message => message.Contains("settlement rejected")), Is.True);
        Assert.That(log.Messages.Any(message => message.Contains("acknowledged") || message.Contains("settled with")), Is.False);
        Assert.That(client.RenewalExited.Task.IsCompletedSuccessfully, Is.True);
    }
}
