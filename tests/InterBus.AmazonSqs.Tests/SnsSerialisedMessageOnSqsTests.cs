using System.Collections.Concurrent;
using System.Text.Json;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Amazon.SQS;
using InterBus.AmazonSqs.Extensions;
using InterBus.Extensions;
using InterBus.Generated;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Testcontainers.LocalStack;

namespace InterBus.AmazonSqs.Tests;

[TestFixture]
public class SnsSerialisedMessageOnSqsTests
{
    private static readonly LocalStackContainer? Container = TestHelpers.CreateLocalStackContainer();

    private ServiceProvider _provider = null!;
    private CancellationTokenSource _cts = null!;
    private SnsSerialisedTracker _tracker = null!;
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

    [TearDown]
    public async Task TearDown()
    {
        if (_provider is null)
            return;

        await _cts.CancelAsync();
        foreach (var svc in _provider.GetServices<IHostedService>())
            await svc.StopAsync(CancellationToken.None);

        _cts.Dispose();

        var sqsClient = _provider.GetRequiredService<IAmazonSQS>();
        var snsClient = _provider.GetRequiredService<IAmazonSimpleNotificationService>();
        var logger = _provider.GetRequiredService<ILogger<SnsSerialisedMessageOnSqsTests>>();
        await TestHelpers.CleanupResourcesByPrefixAsync(sqsClient, snsClient, _queuePrefix, _topicPrefix, logger);
        await _provider.DisposeAsync();
    }

    [Test]
    public async Task Consume_SnsSerialisedMessageOnSqs_PullsHeadersFromSnsMessageAttributes()
    {
        await SetupAsync(
            configureTransport: _ => { },
            configureConsumer: c => c.UseSnsSerialisedMessageOnSqs());

        var sns = _provider.GetRequiredService<IAmazonSimpleNotificationService>();
        var topicName = $"{_topicPrefix}{RoutingConventions.TopicName(typeof(OrderPlaced))}";
        var topicArn = (await sns.CreateTopicAsync(topicName)).TopicArn;

        var order = new OrderPlaced(Guid.NewGuid(), "interop@example.com", 5.20m);
        var payload = JsonSerializer.Serialize(order);
        var messageType = typeof(OrderPlaced).FullName!;
        const string customHeader = "custom-value";

        await sns.PublishAsync(
            new PublishRequest
            {
                TopicArn = topicArn,
                Message = payload,
                MessageAttributes = new Dictionary<string, MessageAttributeValue>
                {
                    [InterBusHeaders.MessageType] = new() { DataType = "String", StringValue = messageType },
                    [InterBusHeaders.MessageId] = new() { DataType = "String", StringValue = Guid.NewGuid().ToString("N") },
                    ["x-custom"] = new() { DataType = "String", StringValue = customHeader },
                },
            },
            TestContext.CurrentContext.CancellationToken);

        await TestHelpers.WaitUntilAsync(() => _tracker.Messages.Count >= 1);

        var consumed = _tracker.Messages.Single();
        using (Assert.EnterMultipleScope())
        {
            Assert.That(consumed.Message.OrderId, Is.EqualTo(order.OrderId));
            Assert.That(consumed.Headers["x-custom"], Is.EqualTo(customHeader));
            Assert.That(consumed.Headers[InterBusHeaders.MessageType], Is.EqualTo(messageType));
        }
    }

    [Test]
    public async Task Publish_DefaultSnsSerialisedMessageOnSqs_DeliversWithoutEnvelopeBase64()
    {
        await SetupAsync(
            configureTransport: o => o.UseSnsSerialisedMessageOnSqsByDefault = true,
            configureConsumer: null);

        using var scope = _provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
        var order = new OrderPlaced(Guid.NewGuid(), "publish@example.com", 10m);

        await publisher.PublishAsync(order, TestContext.CurrentContext.CancellationToken);
        await TestHelpers.WaitUntilAsync(() => _tracker.Messages.Count >= 1);

        Assert.That(_tracker.Messages.Single().Message.OrderId, Is.EqualTo(order.OrderId));
    }

    private async Task SetupAsync(
        Action<SnsSqsTransportOptions> configureTransport,
        Action<IConsumerBuilder>? configureConsumer)
    {
        _runId = Guid.NewGuid().ToString("N")[..8];
        _queuePrefix = $"q-{_runId}-";
        _topicPrefix = $"t-{_runId}-";
        _tracker = new SnsSerialisedTracker();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(_tracker);

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
                configureTransport(o);
            });
            bus.AddConsumer<SnsSerialisedOrderPlacedConsumer>(configureConsumer);
        });

        _provider = services.BuildServiceProvider();
        _cts = new CancellationTokenSource();

        foreach (var svc in _provider.GetServices<IHostedService>())
            await svc.StartAsync(_cts.Token);
    }
}

public sealed class SnsSerialisedTracker
{
    private readonly ConcurrentBag<SnsSerialisedConsumedMessage> _messages = [];
    public IReadOnlyCollection<SnsSerialisedConsumedMessage> Messages => _messages;

    public void Track(OrderPlaced message, MessageHeaders headers) =>
        _messages.Add(new SnsSerialisedConsumedMessage(message, headers));
}

public sealed record SnsSerialisedConsumedMessage(OrderPlaced Message, MessageHeaders Headers);

public sealed class SnsSerialisedOrderPlacedConsumer(SnsSerialisedTracker tracker) : IConsumer<OrderPlaced>
{
    public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        tracker.Track(message, new MessageHeaders(context.Headers));
        return Task.CompletedTask;
    }
}
