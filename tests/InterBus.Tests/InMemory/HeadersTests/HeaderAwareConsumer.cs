namespace InterBus.Tests.InMemory;

public partial class HeadersTests
{
    public sealed class HeaderAwareConsumer(HeaderCapture headerCapture) : IConsumer<OrderPlaced>
    {
        public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            headerCapture.Add(context.Headers);
            return Task.CompletedTask;
        }
    }
}
