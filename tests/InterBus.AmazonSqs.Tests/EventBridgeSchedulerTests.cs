using Amazon.Scheduler;
using Amazon.Scheduler.Model;
using Amazon.SecurityToken;
using Amazon.SimpleNotificationService;
using Amazon.SQS;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Testcontainers.LocalStack;

namespace InterBus.AmazonSqs.Tests;

/// <summary>
/// Integration tests for <see cref="EventBridgeSchedulingProvider"/> using LocalStack.
/// </summary>
[TestFixture]
public sealed class EventBridgeSchedulerTests
{
    private static readonly LocalStackContainer? Container = TestHelpers.CreateLocalStackContainer();

    private ServiceProvider _serviceProvider;
    private SnsSqsTopology _topology;
    private SnsSqsTopologyBuilder _topologyBuilder;
    private string _queuePrefix;
    private string _topicPrefix;
    private readonly List<(Guid ScheduleId, string GroupName)> _createdSchedules = [];
    private readonly List<string> _createdGroups = [];

    private IAmazonSQS SqsClient => _serviceProvider.GetRequiredService<IAmazonSQS>();

    private IAmazonSimpleNotificationService SnsClient =>
        _serviceProvider.GetRequiredService<IAmazonSimpleNotificationService>();

    private IAmazonScheduler SchedulerClient => _serviceProvider.GetRequiredService<IAmazonScheduler>();

    private static string TargetRoleArn =>
        Environment.GetEnvironmentVariable("INTERBUS_EVENTBRIDGE_TARGET_ROLE_ARN")
        ?? "arn:aws:iam::123456789012:role/eventbridge-role";

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
    public void Setup()
    {
        var runId = Guid.NewGuid().ToString("N")[..8];
        _queuePrefix = $"q-{runId}-";
        _topicPrefix = $"t-{runId}-";
        _createdSchedules.Clear();
        _createdGroups.Clear();

        var awsOptions = TestHelpers.CreateAwsOptions(Container);
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddAWSService<IAmazonSQS>(awsOptions);
        services.AddAWSService<IAmazonSimpleNotificationService>(awsOptions);
        services.AddAWSService<IAmazonSecurityTokenService>(awsOptions);
        services.AddAWSService<IAmazonScheduler>(awsOptions);
        _serviceProvider = services.BuildServiceProvider();

        var transportOptions = new SnsSqsTransportOptions { QueuePrefix = _queuePrefix, TopicPrefix = _topicPrefix };
        _topology = new SnsSqsTopology(transportOptions);
        var logger = _serviceProvider.GetRequiredService<ILogger<SnsSqsTopologyBuilder>>();
        _topologyBuilder = new SnsSqsTopologyBuilder(
            SnsClient, SqsClient,
            _serviceProvider.GetRequiredService<IAmazonSecurityTokenService>(),
            _topology,
            transportOptions,
            new InterBusOptions(),
            logger);
    }

    [TearDown]
    public async Task TearDown()
    {
        foreach (var (scheduleId, groupName) in _createdSchedules)
        {
            try
            {
                await SchedulerClient.DeleteScheduleAsync(new DeleteScheduleRequest
                {
                    Name = scheduleId.ToString(),
                    GroupName = groupName,
                });
            }
            catch
            {
                /* best-effort cleanup */
            }
        }

        foreach (var groupName in _createdGroups)
        {
            try
            {
                await SchedulerClient.DeleteScheduleGroupAsync(
                    new DeleteScheduleGroupRequest { Name = groupName });
            }
            catch
            {
                /* best-effort cleanup */
            }
        }

        await TestHelpers.CleanupResourcesByPrefixAsync(
            SqsClient, SnsClient, _queuePrefix, _topicPrefix);

        await _serviceProvider.DisposeAsync();
    }

    #region ScheduleAsync - creates EventBridge schedule

    [Test]
    public async Task ScheduleAsync_WithQueueUri_CreatesSchedule_WithSqsTargetArn()
    {
        var ct = TestContext.CurrentContext.CancellationToken;
        await _topologyBuilder.EnsureQueueAsync("my-queue", ct);

        var scheduleId = Guid.CreateVersion7();
        _createdSchedules.Add((scheduleId, "default"));
        await BuildProvider().ScheduleAsync(
            scheduleId, new Uri("sqsqueue://interbus/my-queue"),
            [1, 2, 3], new MessageHeaders { [InterBusHeaders.MessageType] = "MyEvent" },
            DateTimeOffset.UtcNow.AddHours(1), ct);

        var schedule = await SchedulerClient.GetScheduleAsync(
            new GetScheduleRequest { Name = scheduleId.ToString(), GroupName = "default" }, ct);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(schedule.Target.Arn, Contains.Substring("my-queue"));
            Assert.That(schedule.Target.RoleArn, Is.EqualTo(TargetRoleArn));
            Assert.That(schedule.ActionAfterCompletion, Is.EqualTo(ActionAfterCompletion.DELETE));
            Assert.That(schedule.FlexibleTimeWindow.Mode, Is.EqualTo(FlexibleTimeWindowMode.OFF));
            Assert.That(schedule.ScheduleExpressionTimezone, Is.EqualTo("UTC"));
        }
    }

    [Test]
    public async Task ScheduleAsync_WithTopicUri_CreatesSchedule_WithSnsTargetArn()
    {
        var ct = TestContext.CurrentContext.CancellationToken;
        var topicArn = await _topologyBuilder.EnsureTopicAsync("my-topic", ct);

        var scheduleId = Guid.CreateVersion7();
        _createdSchedules.Add((scheduleId, "default"));
        await BuildProvider().ScheduleAsync(
            scheduleId, new Uri("snstopic://interbus/my-topic"),
            [], new MessageHeaders(), DateTimeOffset.UtcNow.AddHours(1), ct);

        var schedule = await SchedulerClient.GetScheduleAsync(
            new GetScheduleRequest { Name = scheduleId.ToString(), GroupName = "default" }, ct);

        Assert.That(schedule.Target.Arn, Is.EqualTo(topicArn));
    }

    [Test]
    public async Task ScheduleAsync_InputIsSnsSqsMessageEnvelope_ContainsPayloadAndHeaders()
    {
        var ct = TestContext.CurrentContext.CancellationToken;
        await _topologyBuilder.EnsureQueueAsync("test-queue", ct);

        var payload = new byte[] { 10, 20, 30 };
        var headers = new MessageHeaders { [InterBusHeaders.MessageType] = "TestEvent" };
        var scheduleId = Guid.CreateVersion7();
        _createdSchedules.Add((scheduleId, "default"));
        await BuildProvider().ScheduleAsync(
            scheduleId, new Uri("sqsqueue://interbus/test-queue"),
            payload, headers, DateTimeOffset.UtcNow.AddMinutes(5), ct);

        var schedule = await SchedulerClient.GetScheduleAsync(
            new GetScheduleRequest { Name = scheduleId.ToString(), GroupName = "default" }, ct);

        Assert.Multiple(() =>
        {
            Assert.That(schedule.Target.Input, Does.Contain(Convert.ToBase64String(payload)));
            Assert.That(schedule.Target.Input, Does.Contain("TestEvent"));
        });
    }

    [Test]
    public async Task ScheduleAsync_ScheduleExpression_UsesAtFormatWithUtcTime()
    {
        var ct = TestContext.CurrentContext.CancellationToken;
        await _topologyBuilder.EnsureQueueAsync("test", ct);

        var scheduledAt = new DateTimeOffset(2030, 6, 15, 10, 30, 0, TimeSpan.Zero);
        var scheduleId = Guid.CreateVersion7();
        _createdSchedules.Add((scheduleId, "default"));
        await BuildProvider().ScheduleAsync(
            scheduleId, new Uri("sqsqueue://interbus/test"),
            [], new MessageHeaders(), scheduledAt, ct);

        var schedule = await SchedulerClient.GetScheduleAsync(
            new GetScheduleRequest { Name = scheduleId.ToString(), GroupName = "default" }, ct);

        Assert.That(schedule.ScheduleExpression, Is.EqualTo("at(2030-06-15T10:30:00)"));
    }

    [Test]
    public async Task ScheduleAsync_UsesCustomScheduleGroupName()
    {
        const string groupName = "my-custom-group";
        var ct = TestContext.CurrentContext.CancellationToken;
        await _topologyBuilder.EnsureQueueAsync("test", ct);
        await SchedulerClient.CreateScheduleGroupAsync(
            new CreateScheduleGroupRequest { Name = groupName }, ct);
        _createdGroups.Add(groupName);

        var scheduleId = Guid.CreateVersion7();
        _createdSchedules.Add((scheduleId, groupName));
        await BuildProvider(groupName: groupName).ScheduleAsync(
            scheduleId, new Uri("sqsqueue://interbus/test"),
            [], new MessageHeaders(), DateTimeOffset.UtcNow.AddHours(1), ct);

        var schedule = await SchedulerClient.GetScheduleAsync(
            new GetScheduleRequest { Name = scheduleId.ToString(), GroupName = groupName }, ct);

        Assert.That(schedule.GroupName, Is.EqualTo(groupName));
    }

    #endregion

    #region CancelAsync - deletes EventBridge schedule

    [Test]
    public async Task CancelAsync_DeletesSchedule()
    {
        var ct = TestContext.CurrentContext.CancellationToken;
        await _topologyBuilder.EnsureQueueAsync("test", ct);
        var provider = BuildProvider();

        var scheduleId = Guid.CreateVersion7();
        await provider.ScheduleAsync(
            scheduleId, new Uri("sqsqueue://interbus/test"),
            [], new MessageHeaders(), DateTimeOffset.UtcNow.AddHours(1), ct);

        await provider.CancelAsync(scheduleId, ct);

        Assert.ThrowsAsync<ResourceNotFoundException>(() => SchedulerClient.GetScheduleAsync(
            new GetScheduleRequest { Name = scheduleId.ToString(), GroupName = "default" }, ct));
    }

    [Test]
    public async Task CancelAsync_WithCustomGroupName_DeletesFromCorrectGroup()
    {
        const string groupName = "my-group";
        var ct = TestContext.CurrentContext.CancellationToken;
        await _topologyBuilder.EnsureQueueAsync("test", ct);
        await SchedulerClient.CreateScheduleGroupAsync(
            new CreateScheduleGroupRequest { Name = groupName }, ct);
        _createdGroups.Add(groupName);

        var provider = BuildProvider(groupName: groupName);
        var scheduleId = Guid.CreateVersion7();
        await provider.ScheduleAsync(
            scheduleId, new Uri("sqsqueue://interbus/test"),
            [], new MessageHeaders(), DateTimeOffset.UtcNow.AddHours(1), ct);

        await provider.CancelAsync(scheduleId, ct);

        Assert.ThrowsAsync<ResourceNotFoundException>(() => SchedulerClient.GetScheduleAsync(
            new GetScheduleRequest { Name = scheduleId.ToString(), GroupName = groupName }, ct));
    }

    #endregion

    #region Helpers

    private EventBridgeSchedulingProvider BuildProvider(
        string? roleArn = null,
        string groupName = "default") =>
        new(SchedulerClient, _topology, _topologyBuilder, new SnsSqsMessageEnvelope(),
            new EventBridgeSchedulerOptions
            {
                TargetRoleArn = roleArn ?? TargetRoleArn,
                ScheduleGroupName = groupName,
            });

    #endregion
}
