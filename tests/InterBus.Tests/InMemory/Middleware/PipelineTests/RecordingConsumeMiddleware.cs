using InterBus.Consuming;

namespace InterBus.Tests.InMemory.Middleware;

public partial class PipelineTests
{
    public sealed class RecordingConsumeMiddleware(ConsumeMiddlewareCallTracker tracker) : IConsumeMiddleware
    {
        public async Task InvokeAsync<T>(ConsumeContext<T> context, Func<ConsumeContext<T>, CancellationToken, Task> next, CancellationToken cancellationToken = default)
            where T : class
        {
            tracker.Increment();
            await next(context, cancellationToken);
        }
    }
}
