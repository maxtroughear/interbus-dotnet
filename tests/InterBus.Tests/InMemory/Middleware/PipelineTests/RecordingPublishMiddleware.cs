using InterBus.Publishing;

namespace InterBus.Tests.InMemory.Middleware;

public partial class PipelineTests
{
    /// <summary>
    /// Publish middleware that records a call to <see cref = "PublishCallTracker"/>
    /// and appends a header before calling next middleware.
    /// </summary>
    public sealed class RecordingPublishMiddleware(PublishCallTracker tracker) : IPublishMiddleware
    {
        public async Task InvokeAsync<T>(PublishContext<T> context, Func<PublishContext<T>, CancellationToken, Task> next, CancellationToken cancellationToken = default)
            where T : class
        {
            tracker.Record("before-" + context.Destination);
            // Demonstrate header mutation (claim-check-like interception point).
            context.Headers["x-middleware-ran"] = "true";
            await next(context, cancellationToken);
            tracker.Record("after-" + context.Destination);
        }
    }
}
