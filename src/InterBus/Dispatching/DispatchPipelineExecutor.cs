using InterBus.Consuming;
using InterBus.Middleware;

namespace InterBus.Dispatching;

public sealed class DispatchPipelineExecutor(
    IMiddlewareFactory middlewareFactory,
    IServiceProvider serviceProvider,
    ConsumePipelineExecutor consumePipelineExecutor)
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

        async Task TerminalDispatchDelegate(DispatchContext ctx, CancellationToken ct)
        {
            try
            {
                await ctx.Descriptor.Dispatch(ctx, consumePipelineExecutor, serviceProvider, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                ctx.Result = DispatchResult.Release(ex);
            }
        }
    }
}
