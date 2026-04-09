namespace InterBus.Middleware;

public sealed class DispatchPipelineExecutor(
    IEnumerable<IDispatchMiddleware> middlewares,
    IServiceProvider serviceProvider)
{
    private readonly IEnumerable<IDispatchMiddleware> _middlewares = middlewares.Reverse().ToList();

    public Task ExecuteAsync(DispatchContext context, CancellationToken cancellationToken = default)
    {
        var pipeline = TerminalDispatchDelegate;
        foreach (var middleware in _middlewares)
        {
            var next = pipeline;
            pipeline = (ctx, ct) => middleware.InvokeAsync(ctx, next, ct);
        }

        return pipeline(context, cancellationToken);

        Task TerminalDispatchDelegate(DispatchContext ctx, CancellationToken ct)
        {
            return context.Descriptor.Dispatch(context, serviceProvider, cancellationToken);
        }
    }
}
