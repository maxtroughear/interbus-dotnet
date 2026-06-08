using Amazon.SimpleNotificationService;
using Amazon.SQS;
using Amazon.SQS.Model;
using InterBus.AmazonSqs.Extensions;
using InterBus.Extensions;
using InterBus.Generated;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Text.Json;
using Testcontainers.LocalStack;

namespace InterBus.AmazonSqs.Tests;

[TestFixture]
public class DeadLetteringTests
{
    private static readonly LocalStackContainer? Container = TestHelpers.CreateLocalStackContainer();

    private ServiceProvider _provider = null!;
    private CancellationTokenSource _cts = null!;
    private ReceiveTracker _receiveTracker = null!;
    private string _runId = null!;
    private string _queuePrefix = null!;
    private string _topicPrefix = null!;

    [OneTimeSetUp]
    public static async Task OneTimeSetUp()
    {
        if (Container is not null)
            await Container.StartAsync();
    }

    [OneTimeTearDown]
    public static async Task OneTimeTearDown()
    {
        if (Container is not null)
        {
            await Container.StopAsync();
            await Container.DisposeAsync();
        }
    }

    [SetUp]
    public async Task Setup()
    {
        _runId = Guid.NewGuid().ToString("N")[..8];
        _queuePrefix = $"q-{_runId}-";
        _topicPrefix = $"t-{_runId}-";
        _receiveTracker = new ReceiveTracker();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(_receiveTracker);

        var awsOptions = TestHelpers.CreateAwsOptions(Container);

        if (awsOptions is not null)
            services.AddDefaultAWSOptions(awsOptions);

        services.AddAWSService<IAmazonSimpleNotificationService>();
        services.AddAWSService<IAmazonSQS>();
        services.AddAWSService<Amazon.SecurityToken.IAmazonSecurityTokenService>();

        services.AddInterBus(bus =>
        {
            bus.AddAmazonSqsTransport(o =>
            {
                o.QueuePrefix = _queuePrefix;
                o.TopicPrefix = _topicPrefix;
            });
            bus.AddConsumer<DeadLetterOrderPlacedConsumer>();
        });

        _provider = services.BuildServiceProvider();
        _cts = new CancellationTokenSource();

        foreach (var svc in _provider.GetServices<IHostedService>())
            await svc.StartAsync(_cts.Token);
    }

    [TearDown]
    public async Task TearDown()
    {
        await _cts.CancelAsync();

        foreach (var svc in _provider.GetServices<IHostedService>())
            await svc.StopAsync(CancellationToken.None);

        _cts.Dispose();

        var sqsClient = _provider.GetRequiredService<IAmazonSQS>();
        var snsClient = _provider.GetRequiredService<IAmazonSimpleNotificationService>();
        var logger = _provider.GetRequiredService<ILogger<DeadLetteringTests>>();
        await TestHelpers.CleanupResourcesByPrefixAsync(sqsClient, snsClient, _queuePrefix, _topicPrefix, logger);

        await _provider.DisposeAsync();
    }

    [Test]
    public async Task FailedConsumer_MessageIsMovedToDeadLetterQueueWithMetadata()
    {
        using var scope = _provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
        await publisher.PublishAsync(
            new OrderPlaced(Guid.NewGuid(), "deadletter@test.com", 1m),
            TestContext.CurrentContext.CancellationToken);

        var sqsClient = _provider.GetRequiredService<IAmazonSQS>();
        Message? deadLetterMessage = null;
        var deadline = DateTime.UtcNow.AddSeconds(30);
        var deadLetterQueueUrl = await FindDeadLetterQueueUrlAsync(sqsClient);
        while (deadLetterMessage is null && DateTime.UtcNow < deadline)
        {
            if (deadLetterQueueUrl is not null)
            {
                var response = await sqsClient.ReceiveMessageAsync(
                    new ReceiveMessageRequest
                    {
                        QueueUrl = deadLetterQueueUrl,
                        MaxNumberOfMessages = 1,
                        WaitTimeSeconds = 1,
                        MessageAttributeNames = ["All"],
                    });

                deadLetterMessage = (response.Messages ?? [])
                    .FirstOrDefault(message => message.MessageAttributes.ContainsKey(InterBusHeaders.DeadLetterReason));
            }

            if (deadLetterMessage is null)
                await Task.Delay(200, TestContext.CurrentContext.CancellationToken);
        }

        Assert.That(deadLetterMessage, Is.Not.Null);
        Assert.That(deadLetterMessage!.MessageAttributes.ContainsKey(InterBusHeaders.DeadLetterReason), Is.True);
        Assert.That(deadLetterMessage.MessageAttributes.ContainsKey(InterBusHeaders.DeadLetterError), Is.True);
        Assert.That(deadLetterMessage.MessageAttributes.ContainsKey(InterBusHeaders.DeadLetterTime), Is.True);
        Assert.That(_receiveTracker.Count, Is.EqualTo(1));
    }

    private async Task<string?> FindDeadLetterQueueUrlAsync(IAmazonSQS sqsClient)
    {
        var listResponse = await sqsClient.ListQueuesAsync(new ListQueuesRequest { QueueNamePrefix = _queuePrefix });
        string? deadLetterTargetArn = null;
        var queueArnsByUrl = new Dictionary<string, string>(StringComparer.Ordinal);

        foreach (var queueUrl in listResponse.QueueUrls ?? [])
        {
            var attrs = await sqsClient.GetQueueAttributesAsync(
                new GetQueueAttributesRequest
                {
                    QueueUrl = queueUrl,
                    AttributeNames = ["QueueArn", "RedrivePolicy"],
                });

            if (attrs.Attributes is null)
                continue;

            if (attrs.Attributes.TryGetValue("QueueArn", out var queueArn) && !string.IsNullOrWhiteSpace(queueArn))
                queueArnsByUrl[queueUrl] = queueArn;

            if (attrs.Attributes.TryGetValue("RedrivePolicy", out var redrivePolicy) &&
                !string.IsNullOrWhiteSpace(redrivePolicy))
            {
                using var doc = JsonDocument.Parse(redrivePolicy);
                if (doc.RootElement.TryGetProperty("deadLetterTargetArn", out var arnElement))
                    deadLetterTargetArn = arnElement.GetString();
            }
        }

        if (!string.IsNullOrWhiteSpace(deadLetterTargetArn))
        {
            return queueArnsByUrl
                .FirstOrDefault(kv => string.Equals(kv.Value, deadLetterTargetArn, StringComparison.Ordinal))
                .Key;
        }

        return queueArnsByUrl.Keys.FirstOrDefault();
    }

    public sealed class ReceiveTracker
    {
        private int _count;
        public int Count => _count;
        public void Increment() => Interlocked.Increment(ref _count);
    }

    public sealed class DeadLetterOrderPlacedConsumer(ReceiveTracker receiveTracker) : IConsumer<OrderPlaced>
    {
        public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            receiveTracker.Increment();
            throw new InvalidOperationException("Simulated dead-letter failure");
        }
    }
}
