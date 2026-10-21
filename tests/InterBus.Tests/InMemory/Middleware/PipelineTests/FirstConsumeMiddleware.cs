using System.Collections.Concurrent;
using InterBus.Consuming;

namespace InterBus.Tests.InMemory.Middleware;

public partial class PipelineTests
{
    /// <summary>Consume middleware that logs entry/exit to a shared queue for ordering tests.</summary>
    public sealed class FirstConsumeMiddleware(ConcurrentQueue<string> log) : IConsumeMiddleware
    {
        public async Task InvokeAsync<T>(ConsumeContext<T> context, Func<ConsumeContext<T>, CancellationToken, Task> next, CancellationToken cancellationToken = default)
            where T : class
        {
            log.Enqueue("first-enter");
            await next(context, cancellationToken);
            log.Enqueue("first-exit");
        }
    }
}
