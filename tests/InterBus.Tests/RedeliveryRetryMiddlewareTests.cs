using InterBus.Middleware;
using InterBus.Retry;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace InterBus.Tests;

/// <summary>
/// Unit tests for <see cref="RedeliveryRetryConsumeMiddleware"/> and <see cref="RedeliveryRetryOptions"/>.
/// </summary>
public sealed class RedeliveryRetryMiddlewareTests
{
    #region Helpers

    private static ConsumeContext BuildContext(MessageHeaders? headers = null) =>
        BuildContext(
            new ConsumerDescriptor(
                typeof(NoOpConsumer),
                "test-queue",
                [typeof(TestMessage)],
                (_, _, _, _) => Task.CompletedTask),
            headers ?? []);

    private static ConsumeContext BuildContext(ConsumerDescriptor descriptor, MessageHeaders headers) =>
        TestConsumeContextFactory.Create(descriptor, headers);

    private static RedeliveryRetryConsumeMiddleware CreateMiddleware(
        RedeliveryRetryOptions options,
        TestTransport transport) =>
        new(options, transport, NullLogger<RedeliveryRetryConsumeMiddleware>.Instance);

    #endregion

    [Fact]
    public async Task DoesNotRedeliver_WhenNextSucceeds()
    {
        var transport = new TestTransport();
        var middleware = CreateMiddleware(new RedeliveryRetryOptions { RetryCount = 3 }, transport);
        var callCount = 0;

        await middleware.InvokeAsync(BuildContext(), (_, _) =>
        {
            callCount++;
            return Task.CompletedTask;
        }, TestContext.Current.CancellationToken);

        Assert.Equal(1, callCount);
        Assert.Empty(transport.Sent);
    }

    [Fact]
    public async Task Redelivers_WhenExceptionThrown_AndRetryAvailable()
    {
        var transport = new TestTransport();
        var middleware = CreateMiddleware(new RedeliveryRetryOptions { RetryCount = 3 }, transport);

        await middleware.InvokeAsync(BuildContext(), (_, _) =>
                throw new InvalidOperationException("boom"),
            TestContext.Current.CancellationToken);

        var sent = Assert.Single(transport.Sent);
        Assert.Equal("test-queue", sent.QueueName);
        Assert.NotEmpty(sent.Payload);
        Assert.Equal("1", sent.Headers[RedeliveryRetryConsumeMiddleware.RetryCountHeader]);
    }

    [Fact]
    public async Task Redelivery_IncrementsExistingRetryHeader()
    {
        var headers = new MessageHeaders { [RedeliveryRetryConsumeMiddleware.RetryCountHeader] = "1" };
        var transport = new TestTransport();
        var middleware = CreateMiddleware(new RedeliveryRetryOptions { RetryCount = 3 }, transport);

        await middleware.InvokeAsync(BuildContext(headers), (_, _) =>
                throw new InvalidOperationException("boom"),
            TestContext.Current.CancellationToken);

        var sent = Assert.Single(transport.Sent);
        Assert.Equal("2", sent.Headers[RedeliveryRetryConsumeMiddleware.RetryCountHeader]);
    }

    [Fact]
    public async Task Rethrows_WhenRetryLimitExceeded()
    {
        var headers = new MessageHeaders { [RedeliveryRetryConsumeMiddleware.RetryCountHeader] = "3" };
        var transport = new TestTransport();
        var middleware = CreateMiddleware(new RedeliveryRetryOptions { RetryCount = 3 }, transport);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            middleware.InvokeAsync(BuildContext(headers), (_, _) =>
                    throw new InvalidOperationException("terminal"),
                TestContext.Current.CancellationToken));

        Assert.Empty(transport.Sent);
    }

    [Fact]
    public async Task PassesDelay_ToTransport_WhenSchedulingRedelivery()
    {
        var delay = TimeSpan.FromSeconds(30);
        var options = new RedeliveryRetryOptions
        {
            RetryCount = 2,
            DelayProvider = RetryDelays.Constant(delay)
        };
        var transport = new TestTransport();
        var middleware = CreateMiddleware(options, transport);

        await middleware.InvokeAsync(BuildContext(), (_, _) =>
                throw new InvalidOperationException("boom"),
            TestContext.Current.CancellationToken);

        var sent = Assert.Single(transport.Sent);
        Assert.Equal(delay, sent.Delay);
    }

    [Fact]
    public async Task DoesNotRedeliver_OnOperationCanceledException_WhenTokenCancelled()
    {
        var transport = new TestTransport();
        var middleware = CreateMiddleware(new RedeliveryRetryOptions { RetryCount = 3 }, transport);
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            middleware.InvokeAsync(BuildContext(), (_, _) =>
                    throw new OperationCanceledException(),
                cts.Token));

        Assert.Empty(transport.Sent);
    }

    [Fact]
    public async Task ShouldRetry_Predicate_BlocksRedelivery_WhenFalse()
    {
        var options = new RedeliveryRetryOptions
        {
            RetryCount = 3,
            ShouldRetry = ex => ex is InvalidOperationException
        };
        var transport = new TestTransport();
        var middleware = CreateMiddleware(options, transport);

        await Assert.ThrowsAsync<ArgumentException>(() =>
            middleware.InvokeAsync(BuildContext(), (_, _) =>
                    throw new ArgumentException("not retried"),
                TestContext.Current.CancellationToken));

        Assert.Empty(transport.Sent);
    }

    [Fact]
    public void RedeliveryRetryOptions_RetryCount_ThrowsOnZero()
    {
        var options = new RedeliveryRetryOptions();
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

    private sealed class TestTransport : IInterBusTransport
    {
        public List<SentMessage> Sent { get; } = [];

        public Task PublishToTopicAsync(string topicName, byte[] payload, MessageHeaders headers,
            CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public Task SendToQueueAsync(string queueName, byte[] payload, MessageHeaders headers,
            CancellationToken cancellationToken)
        {
            Sent.Add(new SentMessage(queueName, payload, headers, TimeSpan.Zero));
            return Task.CompletedTask;
        }

        public Task SendToQueueAsync(string queueName, byte[] payload, MessageHeaders headers,
            TimeSpan delay, CancellationToken cancellationToken)
        {
            Sent.Add(new SentMessage(queueName, payload, headers, delay));
            return Task.CompletedTask;
        }
    }

    private sealed record SentMessage(string QueueName, byte[] Payload, MessageHeaders Headers, TimeSpan Delay);

    #endregion
}
