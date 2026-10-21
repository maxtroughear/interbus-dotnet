namespace InterBus.Tests.InMemory.Middleware;

public partial class PipelineTests
{
    public sealed class HeaderCapturingConsumer(HeaderCapturingTracker tracker) : IConsumer<OrderPlaced>
    {
        public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            tracker.Capture(context.Headers);
            return Task.CompletedTask;
        }
    }
}
