using System.Collections.Concurrent;
using Amazon;
using Amazon.Extensions.NETCore.Setup;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using InterBus.Extensions;
using InterBus.Generated;
using InterBus.InMemory.Extensions;
using InterBus.S3.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Testcontainers.LocalStack;

namespace InterBus.S3.Tests;

// ---------------------------------------------------------------------------
// Message types
// ---------------------------------------------------------------------------

public record LargePayloadMessage(Guid MessageId, string Content);

// ---------------------------------------------------------------------------
// Trackers and consumers
// ---------------------------------------------------------------------------

public sealed class LargePayloadTracker
{
    private readonly ConcurrentBag<LargePayloadMessage> _messages = [];
    public IReadOnlyCollection<LargePayloadMessage> Messages => _messages;
    public void Track(LargePayloadMessage message) => _messages.Add(message);
}

public sealed class LargePayloadConsumer(LargePayloadTracker tracker) : IConsumer<LargePayloadMessage>
{
    public Task Consume(LargePayloadMessage message, IConsumeContext context,
        CancellationToken cancellationToken = default)
    {
        tracker.Track(message);
        return Task.CompletedTask;
    }
}

// ---------------------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------------------

[TestFixture]
public sealed class S3ClaimCheckTests
{
    private static readonly LocalStackContainer? Container = new LocalStackBuilder("localstack/localstack:4").Build();

    private IAmazonS3 _s3Client = null!;
    private string _bucketName = string.Empty;

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
    public async Task SetUp()
    {
        var awsOptions = CreateAwsOptions();
        var services = new ServiceCollection();
        services.AddDefaultAWSOptions(awsOptions);
        services.AddAWSService<IAmazonS3>();

        var provider = services.BuildServiceProvider();
        _s3Client = provider.GetRequiredService<IAmazonS3>();

        _bucketName = $"interbus-test-{Guid.NewGuid():N}";
        await _s3Client.PutBucketAsync(new PutBucketRequest { BucketName = _bucketName });
    }

    [TearDown]
    public async Task TearDown()
    {
        if (_s3Client is null || string.IsNullOrEmpty(_bucketName))
            return;

        // Delete all objects in the bucket then delete the bucket.
        var objects = await _s3Client.ListObjectsV2Async(new ListObjectsV2Request { BucketName = _bucketName });
        if (objects.S3Objects?.Count > 0)
        {
            await _s3Client.DeleteObjectsAsync(new DeleteObjectsRequest
            {
                BucketName = _bucketName,
                Objects = objects.S3Objects.Select(o => new KeyVersion { Key = o.Key }).ToList(),
            });
        }

        await _s3Client.DeleteBucketAsync(_bucketName);
        _s3Client.Dispose();
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    /// <summary>
    /// A message whose payload is below the size threshold is forwarded to the transport
    /// unchanged; no S3 object is created.
    /// </summary>
    [Test]
    public async Task Publish_SmallMessage_DeliveredWithoutClaimCheck()
    {
        var tracker = new LargePayloadTracker();

        var (provider, cts) = await StartBusAsync(
            services => services.AddSingleton(tracker),
            _bucketName,
            options => options.SizeThresholdBytes = 256 * 1024);

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            var msg = new LargePayloadMessage(Guid.NewGuid(), "small");

            await publisher.PublishAsync(msg, TestContext.CurrentContext.CancellationToken);

            await WaitUntilAsync(() => tracker.Messages.Count >= 1);

            Assert.That(tracker.Messages, Has.Count.EqualTo(1));
            Assert.That(tracker.Messages.Single().MessageId, Is.EqualTo(msg.MessageId));

            // No objects should have been stored in S3.
            var objects = await _s3Client.ListObjectsV2Async(
                new ListObjectsV2Request { BucketName = _bucketName },
                TestContext.CurrentContext.CancellationToken);
            Assert.That(objects.S3Objects?.Count ?? 0, Is.EqualTo(0));
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    /// <summary>
    /// A message whose payload exceeds the size threshold is offloaded to S3 and the
    /// consumer still receives the original content after the dispatch middleware fetches it.
    /// </summary>
    [Test]
    public async Task Publish_LargeMessage_StoredInS3AndDeliveredToConsumer()
    {
        var tracker = new LargePayloadTracker();

        // Set a very small threshold so any message triggers the claim check.
        var (provider, cts) = await StartBusAsync(
            services => services.AddSingleton(tracker),
            _bucketName,
            options => options.SizeThresholdBytes = 1);

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            var msg = new LargePayloadMessage(Guid.NewGuid(), new string('x', 1024));

            await publisher.PublishAsync(msg, TestContext.CurrentContext.CancellationToken);

            await WaitUntilAsync(() => tracker.Messages.Count >= 1);

            Assert.That(tracker.Messages, Has.Count.EqualTo(1));
            Assert.That(tracker.Messages.Single().MessageId, Is.EqualTo(msg.MessageId));
            Assert.That(tracker.Messages.Single().Content, Is.EqualTo(msg.Content));

            // An S3 object should have been stored.
            var objects = await _s3Client.ListObjectsV2Async(
                new ListObjectsV2Request { BucketName = _bucketName },
                TestContext.CurrentContext.CancellationToken);
            Assert.That(objects.S3Objects, Has.Count.EqualTo(1));
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    /// <summary>
    /// Sending directly to a consumer's queue with a large payload also uses the claim check.
    /// </summary>
    [Test]
    public async Task Send_LargeMessage_StoredInS3AndDeliveredToConsumer()
    {
        var tracker = new LargePayloadTracker();

        var (provider, cts) = await StartBusAsync(
            services => services.AddSingleton(tracker),
            _bucketName,
            options => options.SizeThresholdBytes = 1);

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            var msg = new LargePayloadMessage(Guid.NewGuid(), new string('y', 2048));

            await publisher.SendAsync<LargePayloadConsumer, LargePayloadMessage>(
                msg, TestContext.CurrentContext.CancellationToken);

            await WaitUntilAsync(() => tracker.Messages.Count >= 1);

            Assert.That(tracker.Messages, Has.Count.EqualTo(1));
            Assert.That(tracker.Messages.Single().MessageId, Is.EqualTo(msg.MessageId));
            Assert.That(tracker.Messages.Single().Content, Is.EqualTo(msg.Content));
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private AWSOptions CreateAwsOptions() => new()
    {
        Credentials = new BasicAWSCredentials("test", "test"),
        Region = RegionEndpoint.USEast1,
        DefaultClientConfig = { ServiceURL = Container!.GetConnectionString() },
    };

    private async Task<(ServiceProvider Provider, CancellationTokenSource Cts)> StartBusAsync(
        Action<IServiceCollection> registerServices,
        string bucketName,
        Action<S3ClaimCheckOptions>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDefaultAWSOptions(CreateAwsOptions());
        services.AddAWSService<IAmazonS3>();

        registerServices(services);

        services.AddInterBus(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddS3ClaimCheck(bucketName, configure);
            bus.AddConsumer<LargePayloadConsumer>(c => c.UseS3ClaimCheck());
        });

        var provider = services.BuildServiceProvider();
        var cts = new CancellationTokenSource();

        foreach (var svc in provider.GetServices<IHostedService>())
            await svc.StartAsync(cts.Token);

        return (provider, cts);
    }

    private static async Task StopAsync(ServiceProvider provider, CancellationTokenSource cts)
    {
        await cts.CancelAsync();
        foreach (var svc in provider.GetServices<IHostedService>())
        {
            try { await svc.StopAsync(CancellationToken.None); }
            catch (OperationCanceledException) { }
        }

        await provider.DisposeAsync();
        cts.Dispose();
    }

    private static async Task WaitUntilAsync(
        Func<bool> condition,
        int timeoutMs = 15_000,
        int intervalMs = 100)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (!condition() && DateTime.UtcNow < deadline)
            await Task.Delay(intervalMs);
    }
}
