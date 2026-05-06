using InterBus.Middleware;

namespace InterBus.Dispatching;

public sealed class DispatchPipelineExecutor(
    IMiddlewareFactory middlewareFactory,
    IServiceProvider serviceProvider)
{
    public Task ExecuteAsync(DispatchContext context, CancellationToken cancellationToken = default)
    {
        var pipeline = TerminalDispatchDelegate;

        foreach (var middleware in middlewareFactory.Resolve<IDispatchMiddleware>(context.Descriptor.ConsumerType))
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
