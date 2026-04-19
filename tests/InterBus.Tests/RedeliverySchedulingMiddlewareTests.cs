using InterBus.Consumer;
using InterBus.Publisher;
using InterBus.Retry;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace InterBus.Tests;

/// <summary>
/// Unit tests for <see cref="RedeliverySchedulingConsumeMiddleware"/> and <see cref="RedeliveryOptions"/>.
/// </summary>
public sealed class RedeliverySchedulingMiddlewareTests
{
    #region Helpers

    private static ConsumeContext<TestMessage> BuildContext(MessageHeaders? headers = null) =>
        BuildContext<TestMessage>(
            new ConsumerDescriptor(
                typeof(NoOpConsumer),
                RoutingConventions.QueueUri("queue", "test-queue"),
                [typeof(TestMessage)],
                (_, _, _) => Task.CompletedTask),
            headers ?? []);

    private static ConsumeContext<T> BuildContext<T>(ConsumerDescriptor descriptor, MessageHeaders headers)
        where T : new() => TestConsumeContextFactory.Create<T>(descriptor, headers);

    private static RedeliverySchedulingConsumeMiddleware CreateMiddleware(
        RedeliveryOptions options,
        IScheduler scheduler,
        IPublisher publisher) =>
        new(options, scheduler, publisher, NullLogger<RedeliverySchedulingConsumeMiddleware>.Instance);

    #endregion

    [Fact]
    public async Task DoesNotRedeliver_WhenNextSucceeds()
    {
        var publisher = new TestPublisher();
        var scheduler = new TestScheduler();
        var middleware = CreateMiddleware(new RedeliveryOptions { RetryCount = 3 }, scheduler, publisher);
        var callCount = 0;

        await middleware.InvokeAsync(BuildContext(), (_, _) =>
        {
            callCount++;
            return Task.CompletedTask;
        }, TestContext.Current.CancellationToken);

        Assert.Equal(1, callCount);
        Assert.Empty(publisher.Sent);
    }

    [Fact]
    public async Task Redelivers_WhenExceptionThrown_AndRetryAvailable()
    {
        var publisher = new TestPublisher();
        var scheduler = new TestScheduler();
        var middleware = CreateMiddleware(new RedeliveryOptions { RetryCount = 3 }, scheduler, publisher);

        await middleware.InvokeAsync(BuildContext(), (_, _) =>
                throw new InvalidOperationException("boom"),
            TestContext.Current.CancellationToken);

        var sent = Assert.Single(publisher.Sent);
        Assert.Equal("test-queue", sent.QueueName);
        Assert.NotNull(sent.Payload);
        Assert.Equal("1", sent.Headers[RedeliverySchedulingConsumeMiddleware.RetryCountHeader]);
    }

    [Fact]
    public async Task Redelivery_IncrementsExistingRetryHeader()
    {
        var headers = new MessageHeaders { [RedeliverySchedulingConsumeMiddleware.RetryCountHeader] = "1" };
        var publisher = new TestPublisher();
        var scheduler = new TestScheduler();
        var middleware = CreateMiddleware(new RedeliveryOptions { RetryCount = 3 }, scheduler, publisher);

        await middleware.InvokeAsync(BuildContext(headers), (_, _) =>
                throw new InvalidOperationException("boom"),
            TestContext.Current.CancellationToken);

        var sent = Assert.Single(publisher.Sent);
        Assert.Equal("2", sent.Headers[RedeliverySchedulingConsumeMiddleware.RetryCountHeader]);
    }

    [Fact]
    public async Task Rethrows_WhenRetryLimitExceeded()
    {
        var headers = new MessageHeaders { [RedeliverySchedulingConsumeMiddleware.RetryCountHeader] = "3" };
        var publisher = new TestPublisher();
        var scheduler = new TestScheduler();
        var middleware = CreateMiddleware(new RedeliveryOptions { RetryCount = 3 }, scheduler, publisher);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            middleware.InvokeAsync(BuildContext(headers), (_, _) =>
                    throw new InvalidOperationException("terminal"),
                TestContext.Current.CancellationToken));

        Assert.Empty(publisher.Sent);
    }

    [Fact]
    public async Task UsesScheduler_WhenDelayIsNonZeroAndSchedulerRegistered()
    {
        var delay = TimeSpan.FromMinutes(5);
        var options = new RedeliveryOptions
        {
            RetryCount = 2,
            DelayProvider = RetryDelays.Constant(delay)
        };
        var publisher = new TestPublisher();
        var scheduler = new TestScheduler();
        var middleware = CreateMiddleware(options, scheduler, publisher);

        await middleware.InvokeAsync(BuildContext(), (_, _) =>
                throw new InvalidOperationException("boom"),
            TestContext.Current.CancellationToken);

        Assert.Empty(publisher.Sent);
        var scheduled = Assert.Single(scheduler.Scheduled);
        Assert.Equal("test-queue", scheduled.QueueName);
        Assert.NotNull(scheduled.Payload);
        Assert.Equal("1", scheduled.Headers[RedeliverySchedulingConsumeMiddleware.RetryCountHeader]);
        Assert.True(scheduled.ScheduledAt > DateTimeOffset.UtcNow, "ScheduledAt should be in the future");
    }

    [Fact]
    public async Task SendsToTransport_WhenDelayIsZeroEvenIfSchedulerRegistered()
    {
        var options = new RedeliveryOptions
        {
            RetryCount = 2,
            DelayProvider = RetryDelays.None
        };
        var publisher = new TestPublisher();
        var scheduler = new TestScheduler();
        var middleware = CreateMiddleware(options, scheduler, publisher);

        await middleware.InvokeAsync(BuildContext(), (_, _) =>
                throw new InvalidOperationException("boom"),
            TestContext.Current.CancellationToken);

        var sent = Assert.Single(publisher.Sent);
        Assert.Equal("test-queue", sent.QueueName);
        Assert.Empty(scheduler.Scheduled);
    }

    [Fact]
    public async Task DoesNotRedeliver_OnOperationCanceledException_WhenTokenCancelled()
    {
        var publisher = new TestPublisher();
        var scheduler = new TestScheduler();
        var middleware = CreateMiddleware(new RedeliveryOptions { RetryCount = 3 }, scheduler, publisher);
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            middleware.InvokeAsync(BuildContext(), (_, _) =>
                    throw new OperationCanceledException(),
                cts.Token));

        Assert.Empty(publisher.Sent);
    }

    [Fact]
    public async Task ShouldRetry_Predicate_BlocksRedelivery_WhenFalse()
    {
        var options = new RedeliveryOptions
        {
            RetryCount = 3,
            ShouldRetry = ex => ex is InvalidOperationException
        };
        var publisher = new TestPublisher();
        var scheduler = new TestScheduler();
        var middleware = CreateMiddleware(options, scheduler, publisher);

        await Assert.ThrowsAsync<ArgumentException>(() =>
            middleware.InvokeAsync(BuildContext(), (_, _) =>
                    throw new ArgumentException("not retried"),
                TestContext.Current.CancellationToken));

        Assert.Empty(publisher.Sent);
    }

    [Fact]
    public void RedeliveryRetryOptions_RetryCount_ThrowsOnZero()
    {
        var options = new RedeliveryOptions();
        Assert.Throws<ArgumentOutOfRangeException>(() => options.RetryCount = 0);
    }

    #region Support types

    private sealed class TestMessage;

    private sealed class NoOpConsumer : IConsumer<TestMessage>
    {
        public Task Consume(TestMessage message, IConsumeContext context,
            CancellationToken cancellationToken = default)
            => Task.CompletedTask;
    }

    private sealed class TestPublisher : IPublisher
    {
        public List<SentMessage<object>> Sent { get; } = [];

        public Task PublishAsync<T>(T message, CancellationToken cancellationToken = default) where T : class
            => Task.CompletedTask;

        public Task PublishAsync<T>(T message, Action<PublishContext<T>> configure,
            CancellationToken cancellationToken = default) where T : class
            => Task.CompletedTask;

        public Task SendAsync<TConsumer, T>(T message, CancellationToken cancellationToken = default)
            where TConsumer : IConsumer<T> where T : class
            => Task.CompletedTask;

        public Task SendAsync<TConsumer, T>(T message, Action<PublishContext<T>> configure,
            CancellationToken cancellationToken = default)
            where TConsumer : IConsumer<T> where T : class
            => Task.CompletedTask;

        public Task SendAsync<T>(string queueName, T message, CancellationToken cancellationToken = default)
            where T : class
            => Task.CompletedTask;

        public Task SendAsync<T>(string queueName, T message, Action<PublishContext<T>> configure,
            CancellationToken cancellationToken = default) where T : class
            => SendAsync(new Uri("queue://interbus/" + queueName), message, configure, cancellationToken);

        public Task SendAsync<T>(Uri queue, T message, CancellationToken cancellationToken = default) where T : class =>
            Task.CompletedTask;

        public Task SendAsync<T>(Uri queue, T message, Action<PublishContext<T>> configure,
            CancellationToken cancellationToken = default) where T : class
        {
            var ctx = new PublishContext<T>(queue, message, new MessageHeaders());
            configure(ctx);
            Sent.Add(new SentMessage<object>(RoutingConventions.GetDestinationName(queue), message, ctx.Headers));
            return Task.CompletedTask;
        }
    }

    private sealed class TestScheduler : IScheduler
    {
        public List<ScheduledMessage<object>> Scheduled { get; } = [];

        public Task SchedulePublishAsync<T>(T message, DateTimeOffset scheduledAt,
            CancellationToken cancellationToken = default) where T : class => Task.CompletedTask;

        public Task SchedulePublishAsync<T>(T message, DateTimeOffset scheduledAt, Action<PublishContext<T>> configure,
            CancellationToken cancellationToken = default) where T : class => Task.CompletedTask;

        public Task ScheduleSendAsync<TConsumer, T>(T message, DateTimeOffset scheduledAt,
            CancellationToken cancellationToken = default) where TConsumer : IConsumer<T> where T : class =>
            Task.CompletedTask;

        public Task ScheduleSendAsync<TConsumer, T>(T message, DateTimeOffset scheduledAt,
            Action<PublishContext<T>> configure,
            CancellationToken cancellationToken = default) where TConsumer : IConsumer<T> where T : class =>
            Task.CompletedTask;

        public Task ScheduleSendAsync<T>(string queueName, T message, DateTimeOffset scheduledAt,
            CancellationToken cancellationToken = default) where T : class => Task.CompletedTask;

        public Task ScheduleSendAsync<T>(string queueName, T message, DateTimeOffset scheduledAt,
            Action<PublishContext<T>> configure,
            CancellationToken cancellationToken = default) where T : class =>
            ScheduleSendAsync(new Uri("queue://interbus/" + queueName), message, scheduledAt, configure,
                cancellationToken);


        public Task ScheduleSendAsync<T>(Uri queue, T message, DateTimeOffset scheduledAt,
            Action<PublishContext<T>> configure,
            CancellationToken cancellationToken = default) where T : class
        {
            var context = new PublishContext<T>(queue, message, new MessageHeaders());
            configure(context);
            Scheduled.Add(new ScheduledMessage<object>(RoutingConventions.GetDestinationName(queue), message,
                context.Headers, scheduledAt));
            return Task.CompletedTask;
        }
    }

    private sealed record SentMessage<T>(string QueueName, T Payload, MessageHeaders Headers);

    private sealed record ScheduledMessage<T>(
        string QueueName,
        T Payload,
        MessageHeaders Headers,
        DateTimeOffset ScheduledAt);

    #endregion
}
