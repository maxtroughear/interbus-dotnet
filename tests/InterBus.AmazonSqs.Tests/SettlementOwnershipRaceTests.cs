using System.Collections.Concurrent;
using Amazon;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using InterBus.AmazonSqs.MessagePumps;
using InterBus.Consuming;
using InterBus.Dispatching;
using InterBus.Extensions;
using InterBus.MessagePumps;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace InterBus.AmazonSqs.Tests;

[TestFixture]
public class SettlementOwnershipRaceTests
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

    private sealed class RaceConsumer : IConsumer;
    private sealed class CaptureLog : ILogger<SqsPrefetchedMessage>, ILogger<DeliveryProcessor>
    {
        public ConcurrentQueue<string> Messages { get; } = new();
        public TaskCompletionSource OwnershipLost { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public bool IsEnabled(LogLevel level) => true;
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public void Log<TState>(LogLevel level, EventId id, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            var message = formatter(state, exception);
            Messages.Enqueue(message);
            if (message.Contains("ownership renewal failed")) OwnershipLost.TrySetResult();
        }
    }
    private sealed class ControlledSqs(string operation) : AmazonSQSClient(new AnonymousAWSCredentials(), RegionEndpoint.USEast1)
    {
        public TaskCompletionSource OperationEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource AllowOperation { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource RenewalEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource FailRenewal { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource RenewalExited { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int Deletes, ImmediateReleases;
        public override async Task<SendMessageResponse> SendMessageAsync(SendMessageRequest request, CancellationToken cancellationToken = default)
        {
            if (operation == "dead-letter-send") await BlockAsync(cancellationToken);
            return new();
        }
        public override async Task<DeleteMessageResponse> DeleteMessageAsync(DeleteMessageRequest request, CancellationToken cancellationToken = default)
        {
            Deletes++;
            if (operation != "dead-letter-send") await BlockAsync(cancellationToken);
            return new();
        }
        private async Task BlockAsync(CancellationToken token)
        { OperationEntered.TrySetResult(); await AllowOperation.Task.WaitAsync(token); }
        public override async Task<ChangeMessageVisibilityResponse> ChangeMessageVisibilityAsync(
            ChangeMessageVisibilityRequest request, CancellationToken cancellationToken = default)
        {
            if (request.VisibilityTimeout == 0) ImmediateReleases++;
            RenewalEntered.TrySetResult();
            try
            {
                await FailRenewal.Task.WaitAsync(cancellationToken);
                throw new ReceiptHandleIsInvalidException("lost ownership");
            }
            finally { RenewalExited.TrySetResult(); }
        }
    }
}
