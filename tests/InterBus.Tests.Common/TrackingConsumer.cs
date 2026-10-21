namespace InterBus.Tests.Common;

public abstract class TrackingConsumer<TMessage, TTracker>(TTracker tracker) : IConsumer<TMessage> where TMessage : class where TTracker : MessageTracker<TMessage>
{
    public Task Consume(TMessage message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        tracker.Track(message);
        return Task.CompletedTask;
    }
}
