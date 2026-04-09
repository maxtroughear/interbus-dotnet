using InterBus.Internal;

namespace InterBus.Middleware;

/// <summary>
/// Executes the consume middleware pipeline for a single message dispatch.
/// Resolved from the per-message DI scope created by the message pump so that
/// each middleware is also resolved from the correct scope.
/// Transport packages should resolve this service from the message scope and call
/// <see cref="ExecuteAsync"/> instead of invoking the consumer dispatch delegate directly.
/// </summary>
public sealed class ConsumePipelineExecutor(IMiddlewareFactory middlewareFactory)
{
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

        var middlewares = middlewareFactory.Resolve<IConsumeMiddleware>(context.Descriptor.ConsumerType);

        foreach (var middleware in middlewares)
        {
            var next = pipeline;
            pipeline = (ctx, ct) => middleware.InvokeAsync(ctx, next, ct);
        }

        return pipeline(context, cancellationToken);

        Task TerminalConsumeDelegate(ConsumeContext<T> ctx, CancellationToken ct)
        {
            var consumeContext = new ConsumeContext(ctx.Headers);
            return finalDelegate(ctx.Message, consumeContext, ct);
        }
    }
}
