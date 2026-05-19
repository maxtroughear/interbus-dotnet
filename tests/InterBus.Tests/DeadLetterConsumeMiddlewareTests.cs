using InterBus.Consuming;
using InterBus.DeadLettering;
using InterBus.Publishing;
using Xunit;

namespace InterBus.Tests;

public class DeadLetterConsumeMiddlewareTests
{
    private static ConsumerDescriptor CreateDescriptor() =>
        new(typeof(TestConsumer),
            new Uri("testqueue://dead-letter-consumer"));

    [Fact]
    public async Task InvokeAsync_WhenUnhandledException_ThrowsDeadLetterExceptionAndSetsHeaders()
    {
        var publisher = new RecordingPublisher();
        var middleware = new DeadLetterConsumeMiddleware(publisher);
        var context = new ConsumeContext<TestMessage>(CreateDescriptor(), new TestMessage(), new MessageHeaders());

        var exception = await Assert.ThrowsAsync<DeadLetterException>(() => middleware.InvokeAsync(
            context,
            (_, _) => throw new InvalidOperationException("boom"),
            CancellationToken.None));

        Assert.Equal("Unhandled exception in consume pipeline", exception.Reason);
        Assert.True(context.Headers.ContainsKey(InterBusHeaders.DeadLetterReason));
        Assert.True(context.Headers.ContainsKey(InterBusHeaders.DeadLetterError));
        Assert.True(context.Headers.ContainsKey(InterBusHeaders.DeadLetteredAt));
    }

    [Fact]
    public async Task InvokeAsync_WhenUnhandledException_PublishesDeadLetterMessage()
    {
        var publisher = new RecordingPublisher();
        var middleware = new DeadLetterConsumeMiddleware(publisher);
        var original = new TestMessage();
        var context = new ConsumeContext<TestMessage>(CreateDescriptor(), original, new MessageHeaders());

        await Assert.ThrowsAsync<DeadLetterException>(() => middleware.InvokeAsync(
            context,
            (_, _) => throw new InvalidOperationException("boom"),
            CancellationToken.None));

        var published = Assert.Single(publisher.Published);
        var deadLetter = Assert.IsType<DeadLetter<TestMessage>>(published);
        Assert.Same(original, deadLetter.OriginalMessage);
        Assert.Equal("Unhandled exception in consume pipeline", deadLetter.Reason);
        Assert.NotEmpty(deadLetter.Error);
        Assert.True(deadLetter.DeadLetteredAtUtc <= DateTimeOffset.UtcNow);
    }

    [Fact]
    public async Task InvokeAsync_WhenMessageIsAlreadyDeadLetter_DoesNotPublishNestedDeadLetter()
    {
        var publisher = new RecordingPublisher();
        var middleware = new DeadLetterConsumeMiddleware(publisher);
        var descriptor = new ConsumerDescriptor(
            typeof(TestConsumer),
            new Uri("testqueue://dead-letter-consumer"));
        var deadLetterMessage = new DeadLetter<TestMessage>
        {
            OriginalMessage = new TestMessage(),
            OriginalHeaders = new MessageHeaders()
            {
                [InterBusHeaders.MessageId] = Guid.CreateVersion7().ToString()
            },
            Reason = "original failure",
            Error = "some error",
            DeadLetteredAtUtc = DateTimeOffset.UtcNow,
        };
        var context = new ConsumeContext<DeadLetter<TestMessage>>(descriptor, deadLetterMessage, new MessageHeaders());

        await Assert.ThrowsAsync<DeadLetterException>(() => middleware.InvokeAsync(
            context,
            (_, _) => throw new InvalidOperationException("boom"),
            CancellationToken.None));

        Assert.Empty(publisher.Published);
    }

    [Fact]
    public async Task InvokeAsync_WhenCancellationRequested_PropagatesOperationCanceled()
    {
        var publisher = new RecordingPublisher();
        var middleware = new DeadLetterConsumeMiddleware(publisher);
        var context = new ConsumeContext<TestMessage>(CreateDescriptor(), new TestMessage(), new MessageHeaders());
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => middleware.InvokeAsync(
            context,
            (_, ct) => Task.FromCanceled(ct),
            cts.Token));
    }

    [Fact]
    public async Task InvokeAsync_WhenPublishFails_OriginalDeadLetterExceptionIsStillThrown()
    {
        var publisher = new FailingPublisher();
        var middleware = new DeadLetterConsumeMiddleware(publisher);
        var context = new ConsumeContext<TestMessage>(CreateDescriptor(), new TestMessage(), new MessageHeaders());

        var exception = await Assert.ThrowsAsync<DeadLetterException>(() => middleware.InvokeAsync(
            context,
            (_, _) => throw new InvalidOperationException("boom"),
            CancellationToken.None));

        Assert.Equal("Unhandled exception in consume pipeline", exception.Reason);
    }

    private sealed class TestConsumer : IConsumer<TestMessage>
    {
        public Task Consume(TestMessage message, IConsumeContext context,
            CancellationToken cancellationToken = default) =>
            Task.CompletedTask;
    }

    private sealed record TestMessage;

    /// <summary>
    /// Base stub that provides no-op implementations for all <see cref="IPublisher"/> send overloads,
    /// letting subclasses focus only on the publish behaviour they need to customise.
    /// </summary>
    private abstract class StubPublisher : IPublisher
    {
        public abstract Task PublishAsync<T>(T message, CancellationToken cancellationToken = default) where T : class;

        public abstract Task PublishAsync<T>(T message, Action<PublishContext<T>> configure,
            CancellationToken cancellationToken = default) where T : class;

        public Task SendAsync<TConsumer, T>(T message, CancellationToken cancellationToken = default)
            where TConsumer : IConsume<T> where T : class => Task.CompletedTask;

        public Task SendAsync<TConsumer, T>(T message, Action<PublishContext<T>> configure,
            CancellationToken cancellationToken = default)
            where TConsumer : IConsume<T> where T : class => Task.CompletedTask;

        public Task SendAsync<T>(string queueName, T message, CancellationToken cancellationToken = default)
            where T : class => Task.CompletedTask;

        public Task SendAsync<T>(string queueName, T message, Action<PublishContext<T>> configure,
            CancellationToken cancellationToken = default) where T : class => Task.CompletedTask;

        public Task SendAsync<T>(Uri queue, T message, CancellationToken cancellationToken = default)
            where T : class => Task.CompletedTask;

        public Task SendAsync<T>(Uri queue, T message, Action<PublishContext<T>> configure,
            CancellationToken cancellationToken = default) where T : class => Task.CompletedTask;
    }

    /// <summary>An <see cref="IPublisher"/> stub that records all published messages.</summary>
    private sealed class RecordingPublisher : StubPublisher
    {
        private readonly List<object> _published = [];
        public IReadOnlyList<object> Published => _published;

        public override Task PublishAsync<T>(T message, CancellationToken cancellationToken = default)
        {
            _published.Add(message);
            return Task.CompletedTask;
        }

        public override Task PublishAsync<T>(T message, Action<PublishContext<T>> configure,
            CancellationToken cancellationToken = default)
        {
            _published.Add(message);
            return Task.CompletedTask;
        }
    }

    /// <summary>An <see cref="IPublisher"/> stub that always throws on publish.</summary>
    private sealed class FailingPublisher : StubPublisher
    {
        public override Task PublishAsync<T>(T message, CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("publish failed");

        public override Task PublishAsync<T>(T message, Action<PublishContext<T>> configure,
            CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("publish failed");
    }
}
