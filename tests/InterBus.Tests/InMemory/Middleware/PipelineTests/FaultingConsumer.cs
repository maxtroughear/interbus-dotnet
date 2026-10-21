namespace InterBus.Tests.InMemory.Middleware;

public partial class PipelineTests
{
    public sealed class FaultingConsumer(FaultingConsumerCallCount count) : IConsumer<OrderPlaced>
    {
        public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            count.Increment();
            throw new InvalidOperationException("Intentional test exception from FaultingConsumer");
        }
    }
}
