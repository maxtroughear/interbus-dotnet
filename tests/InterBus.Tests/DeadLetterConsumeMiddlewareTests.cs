using InterBus.Consumer;
using InterBus.DeadLettering;
using InterBus.Middleware;
using InterBus.Send;
using InterBus.Serialization;
using InterBus.Transport;
using Xunit;

namespace InterBus.Tests;

public class DeadLetterConsumeMiddlewareTests
{
    private static ConsumerDescriptor CreateDescriptor() =>
        new(typeof(TestConsumer),
            new Uri("testqueue://dead-letter-consumer"),
            [typeof(TestMessage)],
            (_, _, _) => Task.CompletedTask);

    private static (DeadLetterConsumeMiddleware Middleware, RecordingTransport Transport) CreateMiddleware(
        bool failSend = false)
    {
        var transport = new RecordingTransport(failSend);
        var factory = new FixedTransportFactory(transport);
        var middlewareFactory = new NoOpMiddlewareFactory();
        var sendPipeline = new SendPipelineExecutor(factory, middlewareFactory);
        var serializer = new SystemTextJsonMessageSerializer();
        var options = new InterBusOptions();
        var contextAccessor = new ConsumeContextAccessor();

        return (new DeadLetterConsumeMiddleware(sendPipeline, serializer, options, contextAccessor), transport);
    }

    [Fact]
    public async Task InvokeAsync_WhenUnhandledException_ThrowsDeadLetterExceptionAndSetsHeaders()
    {
        var (middleware, _) = CreateMiddleware();
        var context = new ConsumeContext<TestMessage>(CreateDescriptor(), new TestMessage("x"), new MessageHeaders());

        var exception = await Assert.ThrowsAsync<DeadLetterException>(
            () => middleware.InvokeAsync(
                context,
                (_, _) => throw new InvalidOperationException("boom"),
                CancellationToken.None));

        Assert.Equal("Unhandled exception in consume pipeline", exception.Reason);
        Assert.True(context.Headers.ContainsKey(InterBusHeaders.DeadLetterReason));
        Assert.True(context.Headers.ContainsKey(InterBusHeaders.DeadLetterError));
        Assert.True(context.Headers.ContainsKey(InterBusHeaders.DeadLetteredAt));
    }

    [Fact]
    public async Task InvokeAsync_WhenUnhandledException_SendsDeadLetterMessageToTransport()
    {
        var (middleware, transport) = CreateMiddleware();
        var original = new TestMessage("hello");
        var context = new ConsumeContext<TestMessage>(CreateDescriptor(), original, new MessageHeaders());

        await Assert.ThrowsAsync<DeadLetterException>(
            () => middleware.InvokeAsync(
                context,
                (_, _) => throw new InvalidOperationException("boom"),
                CancellationToken.None));

        var sent = Assert.Single(transport.Sent);
        var serializer = new SystemTextJsonMessageSerializer();
        var deadLetter = serializer.Deserialize<DeadLetter<TestMessage>>(sent.Payload);
        Assert.NotNull(deadLetter);
        Assert.Equal(original.Value, deadLetter.OriginalMessage.Value);
        Assert.Equal("Unhandled exception in consume pipeline", deadLetter.Reason);
        Assert.NotEmpty(deadLetter.Error);
        Assert.True(deadLetter.DeadLetteredAtUtc <= DateTimeOffset.UtcNow);
    }

    [Fact]
    public async Task InvokeAsync_WhenMessageIsAlreadyDeadLetter_DoesNotSendNestedDeadLetter()
    {
        var (middleware, transport) = CreateMiddleware();
        var descriptor = new ConsumerDescriptor(
            typeof(TestConsumer),
            new Uri("testqueue://dead-letter-consumer"),
            [typeof(DeadLetter<TestMessage>)],
            (_, _, _) => Task.CompletedTask);
        var deadLetterMessage = new DeadLetter<TestMessage>
        {
            OriginalMessage = new TestMessage("nested"),
            Reason = "original failure",
            Error = "some error",
            DeadLetteredAtUtc = DateTimeOffset.UtcNow,
        };
        var context = new ConsumeContext<DeadLetter<TestMessage>>(descriptor, deadLetterMessage, new MessageHeaders());

        await Assert.ThrowsAsync<DeadLetterException>(
            () => middleware.InvokeAsync(
                context,
                (_, _) => throw new InvalidOperationException("boom"),
                CancellationToken.None));

        Assert.Empty(transport.Sent);
    }

    [Fact]
    public async Task InvokeAsync_WhenCancellationRequested_PropagatesOperationCanceled()
    {
        var (middleware, _) = CreateMiddleware();
        var context = new ConsumeContext<TestMessage>(CreateDescriptor(), new TestMessage("x"), new MessageHeaders());
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => middleware.InvokeAsync(
                context,
                (_, ct) => Task.FromCanceled(ct),
                cts.Token));
    }

    [Fact]
    public async Task InvokeAsync_WhenSendFails_OriginalDeadLetterExceptionIsStillThrown()
    {
        var (middleware, _) = CreateMiddleware(failSend: true);
        var context = new ConsumeContext<TestMessage>(CreateDescriptor(), new TestMessage("x"), new MessageHeaders());

        var exception = await Assert.ThrowsAsync<DeadLetterException>(
            () => middleware.InvokeAsync(
                context,
                (_, _) => throw new InvalidOperationException("boom"),
                CancellationToken.None));

        Assert.Equal("Unhandled exception in consume pipeline", exception.Reason);
    }

    private sealed class TestConsumer : IConsumer<TestMessage>
    {
        public Task Consume(TestMessage message, IConsumeContext context, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;
    }

    private sealed record TestMessage(string Value);

    private sealed class RecordingTransport(bool fail = false) : ITransport
    {
        private readonly List<TransportSendContext> _sent = [];
        public IReadOnlyList<TransportSendContext> Sent => _sent;

        public Task SendAsync(TransportSendContext context, CancellationToken cancellationToken)
        {
            if (fail)
                throw new InvalidOperationException("send failed");
            _sent.Add(context);
            return Task.CompletedTask;
        }
    }

    private sealed class FixedTransportFactory(ITransport transport) : ITransportFactory
    {
        public ITransport Resolve(Uri destination) => transport;
    }

    private sealed class NoOpMiddlewareFactory : IMiddlewareFactory
    {
        public IEnumerable<TMiddleware> Resolve<TMiddleware>(Type? key = null) where TMiddleware : IMiddleware =>
            [];
    }
}
