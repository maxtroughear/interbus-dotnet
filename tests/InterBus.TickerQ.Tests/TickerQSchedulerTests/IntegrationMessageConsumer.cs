namespace InterBus.TickerQ.Tests;

public sealed partial class TickerQSchedulerTests
{
    internal sealed class IntegrationMessageConsumer(IntegrationMessageTracker tracker) : IConsumer<IntegrationMessage>
    {
        public Task Consume(IntegrationMessage message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            tracker.Track(message);
            return Task.CompletedTask;
        }
    }
}
