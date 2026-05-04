using Amazon.Scheduler;
using Amazon.SecurityToken;
using Amazon.SimpleNotificationService;
using Amazon.SQS;
using InterBus.AmazonSqs.Extensions;
using InterBus.Extensions;
using InterBus.Generated;
using InterBus.Scheduling.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace InterBus.AmazonSqs.Tests;

/// <summary>
/// End-to-end integration tests that verify EventBridge Scheduler actually delivers a message
/// to an SQS queue consumer. These tests target real AWS and are skipped on LocalStack because
/// LocalStack does not execute scheduled deliveries.
/// </summary>
[TestFixture]
public sealed class EventBridgeDeliveryTests
{
    /// <summary>
    /// The dedicated IAM role that EventBridge Scheduler assumes when delivering messages to SQS.
    /// The role must have <c>sqs:SendMessage</c> permission on the target queue and a trust policy
    /// allowing <c>scheduler.amazonaws.com</c> to call <c>sts:AssumeRole</c>.
    /// Read from the <c>INTERBUS_EVENTBRIDGE_TARGET_ROLE_ARN</c> environment variable when running
    /// against real AWS (set in CI via the <c>test-sqs-aws</c> workflow job).
    /// </summary>
    private static string SchedulerRoleArn =>
        Environment.GetEnvironmentVariable("INTERBUS_EVENTBRIDGE_TARGET_ROLE_ARN")
        ?? "arn:aws:iam::123456789012:role/eventbridge-role";

    /// <summary>Hard test timeout in milliseconds (12 minutes).</summary>
    private const int TestTimeoutMs = 720_000;

    /// <summary>
    /// Full end-to-end test: schedules an <see cref="OrderPlaced"/> message to be delivered
    /// directly to the <see cref="OrderPlacedConsumer"/> queue via EventBridge Scheduler, then
    /// verifies the consumer receives it.
    /// <para>
    /// EventBridge Scheduler fires approximately one minute after the scheduled time, so the
    /// message is scheduled 2 minutes from now and delivery is awaited for up to 8 minutes.
    /// </para>
    /// </summary>
    [Test]
    [CancelAfter(TestTimeoutMs)] // 12-minute hard timeout
    public async Task EventBridgeScheduler_DirectSendToConsumerQueue_MessageIsDelivered()
    {
        Assume.That(TestHelpers.UseRealAws, Is.True,
            "Skipped: requires real AWS credentials. Set INTERBUS_USE_REAL_AWS=true to run.");

        var ct = TestContext.CurrentContext.CancellationToken;
        var runId = Guid.NewGuid().ToString("N")[..8];
        var queuePrefix = $"q-eb-{runId}-";
        var topicPrefix = $"t-eb-{runId}-";
        var tracker = new OrderPlacedTracker();

        var services = new ServiceCollection();
        services.AddLogging(b => b.AddConsole().SetMinimumLevel(LogLevel.Debug));
        services.AddSingleton(tracker);

        // Use ambient AWS credentials injected by the OIDC action — no explicit AWSOptions needed.
        services.AddAWSService<IAmazonSimpleNotificationService>();
        services.AddAWSService<IAmazonSQS>();
        services.AddAWSService<IAmazonSecurityTokenService>();
        services.AddAWSService<IAmazonScheduler>();

        services.AddInterBus(bus =>
        {
            bus.AddAmazonSqsTransport(o =>
            {
                o.QueuePrefix = queuePrefix;
                o.TopicPrefix = topicPrefix;
            });
            bus.AddConsumer<OrderPlacedConsumer>();
            bus.AddScheduling(sched =>
                sched.Directly().AddEventBridgeScheduler(opts =>
                {
                    opts.TargetRoleArn = SchedulerRoleArn;
                }));
        });

        await using var provider = services.BuildServiceProvider();
        using var cts = new CancellationTokenSource();

        try
        {
            foreach (var svc in provider.GetServices<IHostedService>())
                await svc.StartAsync(cts.Token);

            // Schedule the message 2 minutes from now to satisfy EventBridge minimum delay.
            var scheduledAt = DateTimeOffset.UtcNow.AddMinutes(2);
            using var scope = provider.CreateScope();
            var scheduler = scope.ServiceProvider.GetRequiredService<IScheduler>();
            var msg = new OrderPlaced(Guid.NewGuid(), "eventbridge@test.com", 42.00m);
            await scheduler.ScheduleSendAsync<OrderPlacedConsumer, OrderPlaced>(msg, scheduledAt, ct);

            // Wait up to 8 minutes — EventBridge fires ~1 min after the scheduled time.
            await TestHelpers.WaitUntilAsync(
                () => tracker.Messages.Count >= 1,
                timeoutMs: 8 * 60 * 1_000,
                intervalMs: 5_000,
                cancellationToken: ct);

            using (Assert.EnterMultipleScope())
            {
                Assert.That(tracker.Messages, Has.Count.EqualTo(1));
                Assert.That(tracker.Messages.Single().OrderId, Is.EqualTo(msg.OrderId));
            }
        }
        finally
        {
            await cts.CancelAsync();

            foreach (var svc in provider.GetServices<IHostedService>())
                await svc.StopAsync(CancellationToken.None);

            var sqsClient = provider.GetRequiredService<IAmazonSQS>();
            var snsClient = provider.GetRequiredService<IAmazonSimpleNotificationService>();
            await TestHelpers.CleanupResourcesByPrefixAsync(sqsClient, snsClient, queuePrefix, topicPrefix);
        }
    }
}
