using InterBus.Middleware;
using InterBus.Transport;

namespace InterBus.Send;

public sealed class SendPipelineExecutor(
    IInterBusTransportFactory transportFactory,
    IMiddlewareFactory middlewareFactory)
{
    /// <summary>
    /// Builds and runs the send pipeline for the given <paramref name="context"/>,
    /// forwarding to the correct <see cref="IInterBusTransport"/> as the terminal step.
    /// </summary>
    public Task ExecuteAsync(SendContext context, Type? key = null, CancellationToken cancellationToken = default)
    {
        var pipeline = DeliverDelegate;

        var middlewares = middlewareFactory.Resolve<ISendMiddleware>(key);
        foreach (var middleware in middlewares)
        {
            var next = pipeline;
            pipeline = (ctx, ct) => middleware.InvokeAsync(ctx, next, ct);
        }

        return pipeline(context, cancellationToken);

        Task DeliverDelegate(SendContext ctx, CancellationToken ct)
        {
            var transport = transportFactory.Resolve(ctx.Destination);
            var sendContext = new TransportSendContext(ctx.Destination, ctx.Payload, ctx.Headers, ctx.Delay);
            return transport.SendAsync(sendContext, ct);
        }
    }
}
