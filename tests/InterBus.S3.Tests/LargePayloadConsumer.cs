namespace InterBus.S3.Tests;

public sealed class LargePayloadConsumer(LargePayloadTracker tracker) : IConsumer<LargePayloadMessage>
{
    public Task Consume(LargePayloadMessage message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        tracker.Track(message);
        return Task.CompletedTask;
    }
}
