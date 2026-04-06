using InterBus.Internal;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.Middleware;

/// <summary>
/// Executes the consume middleware pipeline for a single message dispatch.
/// Resolved from the per-message DI scope created by the message pump so that
/// each middleware is also resolved from the correct scope.
/// Transport packages should resolve this service from the message scope and call
/// <see cref="ExecuteAsync"/> instead of invoking the consumer dispatch delegate directly.
/// </summary>
public sealed class ConsumePipelineExecutor(
    IEnumerable<IConsumeMiddleware> middlewares,
    IServiceProvider serviceProvider)
{
    private readonly IList<IConsumeMiddleware> _middlewares = middlewares.Reverse().ToList();

    /// <summary>
    /// Builds and runs the consume pipeline for the given <paramref name="context"/> and
    /// <paramref name="finalDelegate"/>
    /// </summary>
    public Task ExecuteAsync<T>(
        ConsumeContext<T> context,
        Func<T, IConsumeContext, CancellationToken, Task> finalDelegate,
        CancellationToken cancellationToken = default) where T : class
    {
        var pipeline = TerminalConsumeDelegate;

        // Apply per-consumer middleware (runs after global middleware, closer to the consumer).
        // Iterate in reverse so first-registered is outermost within the per-consumer section.
        for (var i = context.Descriptor.ConsumeMiddlewareTypes.Count - 1; i >= 0; i--)
        {
            var mw = (IConsumeMiddleware)serviceProvider.GetRequiredService(
                context.Descriptor.ConsumeMiddlewareTypes[i]);
            var next = pipeline;
            pipeline = (ctx, ct) => mw.InvokeAsync(ctx, next, ct);
        }

        // Apply global middleware (outermost layer, already reversed).
        foreach (var middleware in _middlewares)
        {
            var current = middleware;
            var next = pipeline;
            pipeline = (ctx, ct) => current.InvokeAsync(ctx, next, ct);
        }

        return pipeline(context, cancellationToken);

        Task TerminalConsumeDelegate(ConsumeContext<T> ctx, CancellationToken ct)
        {
            var publisher = serviceProvider.GetRequiredService<IMessagePublisher>();
            var scheduler = serviceProvider.GetService<IMessageScheduler>();
            var consumeContext = new ConsumeContext(ctx.Headers, publisher, scheduler);
            return finalDelegate(ctx.Payload, consumeContext, ct);
        }
    }
}
