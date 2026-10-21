using System.Collections.Concurrent;
using InterBus.Publishing;

namespace InterBus.Tests.InMemory.Middleware;

public partial class PipelineTests
{
    public sealed class FirstPublishMiddleware(ConcurrentQueue<string> log) : IPublishMiddleware
    {
        public async Task InvokeAsync<T>(PublishContext<T> context, Func<PublishContext<T>, CancellationToken, Task> next, CancellationToken cancellationToken = default)
            where T : class
        {
            log.Enqueue("first-enter");
            await next(context, cancellationToken);
            log.Enqueue("first-exit");
        }
    }
}
