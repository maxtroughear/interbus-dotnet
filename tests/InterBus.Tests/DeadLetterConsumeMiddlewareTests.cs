using InterBus.Consumer;
using InterBus.DeadLettering;
using Xunit;

namespace InterBus.Tests;

public class DeadLetterConsumeMiddlewareTests
{
    [Fact]
    public async Task InvokeAsync_WhenUnhandledException_ThrowsDeadLetterExceptionAndSetsHeaders()
    {
        var middleware = new DeadLetterConsumeMiddleware();
        var descriptor = new ConsumerDescriptor(
            typeof(TestConsumer),
            new Uri("testqueue://dead-letter-consumer"),
            [typeof(TestMessage)],
            (_, _, _) => Task.CompletedTask);
        var context = new ConsumeContext<TestMessage>(descriptor, new TestMessage(), new MessageHeaders());

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
    public async Task InvokeAsync_WhenCancellationRequested_PropagatesOperationCanceled()
    {
        var middleware = new DeadLetterConsumeMiddleware();
        var descriptor = new ConsumerDescriptor(
            typeof(TestConsumer),
            new Uri("testqueue://dead-letter-consumer"),
            [typeof(TestMessage)],
            (_, _, _) => Task.CompletedTask);
        var context = new ConsumeContext<TestMessage>(descriptor, new TestMessage(), new MessageHeaders());
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => middleware.InvokeAsync(
                context,
                (_, ct) => Task.FromCanceled(ct),
                cts.Token));
    }

    private sealed class TestConsumer : IConsumer<TestMessage>
    {
        public Task Consume(TestMessage message, IConsumeContext context, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;
    }

    private sealed record TestMessage;
}
