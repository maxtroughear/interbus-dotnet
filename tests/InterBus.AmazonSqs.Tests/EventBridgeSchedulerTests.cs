using Amazon.Scheduler;
using Amazon.Scheduler.Model;
using InterBus.AmazonSqs.Extensions;
using InterBus.Extensions;
using InterBus.InMemory.Extensions;
using InterBus.Scheduling.Extensions;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.AmazonSqs.Tests;

/// <summary>
/// Unit tests for <see cref="EventBridgeSchedulingProvider"/>.
/// </summary>
[TestFixture]
public sealed class EventBridgeSchedulerTests
{
    #region ScheduleAsync - creates EventBridge schedule

    [Test]
    public async Task ScheduleAsync_WithQueueUri_CreatesSchedule_WithSqsTargetArn()
    {
        var scheduleStore = new StubScheduleStore();
        var topology = BuildTopology(queueArn: ("my-queue", "arn:aws:sqs:us-east-1:123456789012:interbus-my-queue"));
        var provider = BuildProvider(scheduleStore, topology);

        var scheduleId = Guid.CreateVersion7();
        var destination = new Uri("sqsqueue://interbus/my-queue");
        var payload = new byte[] { 1, 2, 3 };
        var headers = new MessageHeaders { [InterBusHeaders.MessageType] = "MyEvent" };
        var scheduledAt = DateTimeOffset.UtcNow.AddHours(1);

        await provider.ScheduleAsync(scheduleId, destination, payload, headers, scheduledAt, CancellationToken.None);

        Assert.That(scheduleStore.CreatedSchedules, Has.Count.EqualTo(1));
        var schedule = scheduleStore.CreatedSchedules[0];
        Assert.That(schedule.Name, Is.EqualTo(scheduleId.ToString()));
        Assert.That(schedule.Target.Arn, Is.EqualTo("arn:aws:sqs:us-east-1:123456789012:interbus-my-queue"));
        Assert.That(schedule.Target.RoleArn, Is.EqualTo("arn:aws:iam::123456789012:role/eventbridge-role"));
        Assert.That(schedule.ActionAfterCompletion, Is.EqualTo(ActionAfterCompletion.DELETE));
        Assert.That(schedule.FlexibleTimeWindow.Mode, Is.EqualTo(FlexibleTimeWindowMode.OFF));
        Assert.That(schedule.ScheduleExpressionTimezone, Is.EqualTo("UTC"));
    }

    [Test]
    public async Task ScheduleAsync_WithTopicUri_CreatesSchedule_WithSnsTargetArn()
    {
        var scheduleStore = new StubScheduleStore();
        var topology = BuildTopology();
        var topologyBuilder = new StubTopologyBuilder(
            topicArn: "arn:aws:sns:us-east-1:123456789012:interbus-my-topic");
        var provider = BuildProvider(scheduleStore, topology, topologyBuilder);

        var scheduleId = Guid.CreateVersion7();
        var destination = new Uri("snstopic://interbus/my-topic");
        var scheduledAt = DateTimeOffset.UtcNow.AddHours(1);

        await provider.ScheduleAsync(scheduleId, destination, [], new MessageHeaders(), scheduledAt, CancellationToken.None);

        var schedule = scheduleStore.CreatedSchedules[0];
        Assert.That(schedule.Target.Arn, Is.EqualTo("arn:aws:sns:us-east-1:123456789012:interbus-my-topic"));
        Assert.That(schedule.Name, Is.EqualTo(scheduleId.ToString()));
    }

    [Test]
    public async Task ScheduleAsync_InputIsSnsSqsMessageEnvelope_ContainsPayloadAndHeaders()
    {
        var scheduleStore = new StubScheduleStore();
        var topology = BuildTopology(queueArn: ("test-queue", "arn:aws:sqs:us-east-1:123456789012:test-queue"));
        var provider = BuildProvider(scheduleStore, topology);

        var payload = new byte[] { 10, 20, 30 };
        var headers = new MessageHeaders { [InterBusHeaders.MessageType] = "TestEvent" };
        var destination = new Uri("sqsqueue://interbus/test-queue");

        await provider.ScheduleAsync(Guid.CreateVersion7(), destination, payload, headers,
            DateTimeOffset.UtcNow.AddMinutes(5), CancellationToken.None);

        var schedule = scheduleStore.CreatedSchedules[0];
        // Input should be a JSON envelope containing the base64 payload and headers.
        Assert.That(schedule.Target.Input, Does.Contain(Convert.ToBase64String(payload)));
        Assert.That(schedule.Target.Input, Does.Contain("TestEvent"));
    }

    [Test]
    public async Task ScheduleAsync_ScheduleExpression_UsesAtFormatWithUtcTime()
    {
        var scheduleStore = new StubScheduleStore();
        var topology = BuildTopology(queueArn: ("test", "arn:aws:sqs:us-east-1:123456789012:test"));
        var provider = BuildProvider(scheduleStore, topology);

        var scheduledAt = new DateTimeOffset(2025, 6, 15, 10, 30, 0, TimeSpan.Zero);
        await provider.ScheduleAsync(
            Guid.CreateVersion7(), new Uri("sqsqueue://interbus/test"), [], new MessageHeaders(), scheduledAt,
            CancellationToken.None);

        var schedule = scheduleStore.CreatedSchedules[0];
        Assert.That(schedule.ScheduleExpression, Is.EqualTo("at(2025-06-15T10:30:00)"));
    }

    [Test]
    public async Task ScheduleAsync_UsesCustomScheduleGroupName()
    {
        var scheduleStore = new StubScheduleStore();
        var topology = BuildTopology(queueArn: ("test", "arn:aws:sqs:us-east-1:123456789012:test"));
        var provider = BuildProvider(scheduleStore, topology, groupName: "my-custom-group");

        await provider.ScheduleAsync(
            Guid.CreateVersion7(), new Uri("sqsqueue://interbus/test"), [], new MessageHeaders(),
            DateTimeOffset.UtcNow.AddHours(1), CancellationToken.None);

        var schedule = scheduleStore.CreatedSchedules[0];
        Assert.That(schedule.GroupName, Is.EqualTo("my-custom-group"));
    }

    [Test]
    public async Task ScheduleAsync_QueueArnComesFromTopology_PopulatedByTopologyBuilder()
    {
        var scheduleStore = new StubScheduleStore();
        // Simulate topology builder having resolved and stored ARNs for two queues.
        var topology = BuildTopology(
            queueArn: ("queue-a", "arn:aws:sqs:us-east-1:123456789012:queue-a"));
        topology.QueueArns.TryAdd("queue-b", "arn:aws:sqs:us-east-1:123456789012:queue-b");
        var provider = BuildProvider(scheduleStore, topology);

        await provider.ScheduleAsync(Guid.CreateVersion7(), new Uri("sqsqueue://interbus/queue-a"), [],
            new MessageHeaders(), DateTimeOffset.UtcNow.AddHours(1), CancellationToken.None);
        await provider.ScheduleAsync(Guid.CreateVersion7(), new Uri("sqsqueue://interbus/queue-b"), [],
            new MessageHeaders(), DateTimeOffset.UtcNow.AddHours(2), CancellationToken.None);

        Assert.That(scheduleStore.CreatedSchedules, Has.Count.EqualTo(2));
        Assert.That(scheduleStore.CreatedSchedules[0].Target.Arn,
            Is.EqualTo("arn:aws:sqs:us-east-1:123456789012:queue-a"));
        Assert.That(scheduleStore.CreatedSchedules[1].Target.Arn,
            Is.EqualTo("arn:aws:sqs:us-east-1:123456789012:queue-b"));
    }

    [Test]
    public void ScheduleAsync_WhenQueueArnMissingFromTopology_ThrowsInvalidOperationException()
    {
        var scheduleStore = new StubScheduleStore();
        var topology = BuildTopology(); // no queue ARNs pre-populated
        var provider = BuildProvider(scheduleStore, topology);

        Assert.ThrowsAsync<InvalidOperationException>(
            () => provider.ScheduleAsync(
                Guid.CreateVersion7(), new Uri("sqsqueue://interbus/unknown-queue"), [], new MessageHeaders(),
                DateTimeOffset.UtcNow.AddHours(1), CancellationToken.None));
    }

    [Test]
    public void ScheduleAsync_WithUnsupportedScheme_ThrowsArgumentException()
    {
        var scheduleStore = new StubScheduleStore();
        var topology = BuildTopology();
        var provider = BuildProvider(scheduleStore, topology);

        Assert.ThrowsAsync<ArgumentException>(
            () => provider.ScheduleAsync(
                Guid.CreateVersion7(), new Uri("memorytopic://interbus/test"), [], new MessageHeaders(),
                DateTimeOffset.UtcNow.AddHours(1), CancellationToken.None));
    }

    #endregion

    #region CancelAsync - deletes EventBridge schedule

    [Test]
    public async Task CancelAsync_SendsDeleteRequest_WithCorrectNameAndGroup()
    {
        var scheduleStore = new StubScheduleStore();
        var topology = BuildTopology();
        var provider = BuildProvider(scheduleStore, topology);

        var scheduleId = Guid.CreateVersion7();
        await provider.CancelAsync(scheduleId, CancellationToken.None);

        Assert.That(scheduleStore.DeletedSchedules, Has.Count.EqualTo(1));
        Assert.That(scheduleStore.DeletedSchedules[0].Name, Is.EqualTo(scheduleId.ToString()));
        Assert.That(scheduleStore.DeletedSchedules[0].GroupName, Is.EqualTo("default"));
    }

    [Test]
    public async Task CancelAsync_UsesCustomScheduleGroupName()
    {
        var scheduleStore = new StubScheduleStore();
        var topology = BuildTopology();
        var provider = BuildProvider(scheduleStore, topology, groupName: "my-group");

        await provider.CancelAsync(Guid.CreateVersion7(), CancellationToken.None);

        Assert.That(scheduleStore.DeletedSchedules[0].GroupName, Is.EqualTo("my-group"));
    }

    #endregion

    #region DI integration

    [Test]
    public void AddEventBridgeScheduler_RegistersEventBridgeSchedulingProvider()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        // Provide real-looking stub for IAmazonScheduler needed by DI
        services.AddSingleton<IAmazonScheduler>(new FakeAmazonSchedulerClient());
        // SnsSqsTopology and its options are normally registered by AddAmazonSqsTransport().
        var transportOptions = new SnsSqsTransportOptions();
        services.AddSingleton(transportOptions);
        services.AddSingleton<SnsSqsTopology>();
        services.AddInterBus(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddScheduling(builder => builder.Directly().AddEventBridgeScheduler(opts =>
            {
                opts.TargetRoleArn = "arn:aws:iam::123456789012:role/test";
            }));
        });

        var provider = services.BuildServiceProvider();
        var schedulingProvider = provider.GetService<IMessageSchedulingProvider>();

        Assert.That(schedulingProvider, Is.Not.Null);
        Assert.That(schedulingProvider, Is.InstanceOf<EventBridgeSchedulingProvider>());
    }

    [Test]
    public void AddEventBridgeScheduler_WithDefaultOptions_UsesDefaultGroup()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IAmazonScheduler>(new FakeAmazonSchedulerClient());
        services.AddSingleton(new SnsSqsTransportOptions());
        services.AddSingleton<SnsSqsTopology>();
        services.AddInterBus(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddScheduling(builder => builder.Directly().AddEventBridgeScheduler());
        });

        var provider = services.BuildServiceProvider();
        var opts = provider.GetRequiredService<EventBridgeSchedulerOptions>();

        Assert.That(opts.ScheduleGroupName, Is.EqualTo("default"));
    }

    #endregion

    #region Helpers

    /// <summary>
    /// Builds a <see cref="SnsSqsTopology"/> with no queue/topic prefix (for predictable key names
    /// in tests) and optionally pre-populates a queue ARN entry (simulating what
    /// <see cref="SnsSqsTopologyBuilder"/> does at startup).
    /// </summary>
    private static SnsSqsTopology BuildTopology((string name, string arn)? queueArn = null)
    {
        // Use empty prefix so BuildSqsQueueName("my-queue") returns "my-queue", not "interbus-my-queue".
        var topology = new SnsSqsTopology(new SnsSqsTransportOptions { QueuePrefix = "", TopicPrefix = "" });
        if (queueArn.HasValue)
            topology.QueueArns.TryAdd(queueArn.Value.name, queueArn.Value.arn);
        return topology;
    }

    private static EventBridgeSchedulingProvider BuildProvider(
        IEventBridgeScheduleStore scheduleStore,
        SnsSqsTopology topology,
        // topologyBuilder is only invoked for SNS topic ARN resolution (EnsureTopicAsync).
        // Tests that schedule to SQS queues can pass null; topic tests must pass a configured stub.
        ITopologyBuilder? topologyBuilder = null,
        string roleArn = "arn:aws:iam::123456789012:role/eventbridge-role",
        string groupName = "default")
    {
        var options = new EventBridgeSchedulerOptions
        {
            TargetRoleArn = roleArn,
            ScheduleGroupName = groupName,
        };
        return new EventBridgeSchedulingProvider(scheduleStore, topology, topologyBuilder ?? new StubTopologyBuilder(), options);
    }

    #endregion

    #region Stub types

    private sealed class StubScheduleStore : IEventBridgeScheduleStore
    {
        public List<CreateScheduleRequest> CreatedSchedules { get; } = [];
        public List<DeleteScheduleRequest> DeletedSchedules { get; } = [];

        public Task CreateScheduleAsync(CreateScheduleRequest request, CancellationToken cancellationToken)
        {
            CreatedSchedules.Add(request);
            return Task.CompletedTask;
        }

        public Task DeleteScheduleAsync(DeleteScheduleRequest request, CancellationToken cancellationToken)
        {
            DeletedSchedules.Add(request);
            return Task.CompletedTask;
        }
    }

    private sealed class StubTopologyBuilder(string? topicArn = null) : ITopologyBuilder
    {
        public Task BuildTopologyAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public Task<string> EnsureTopicAsync(string rawTopicName, CancellationToken cancellationToken) =>
            Task.FromResult(topicArn ?? throw new InvalidOperationException("Topic ARN not configured"));
    }

    /// <summary>Minimal fake Amazon Scheduler client for DI resolution tests only.</summary>
    private sealed class FakeAmazonSchedulerClient : AmazonSchedulerClient
    {
        public FakeAmazonSchedulerClient()
            : base(new Amazon.Runtime.BasicAWSCredentials("test", "test"),
                   Amazon.RegionEndpoint.USEast1)
        {
        }
    }

    #endregion
}
