namespace InterBus.Middleware;

public sealed class SendPipelineExecutor(
    IEnumerable<ISendMiddleware> middlewares,
    IInterBusTransportFactory transportFactory)
{
    private readonly IList<ISendMiddleware> _middlewares = middlewares.Reverse().ToList();

    /// <summary>
    /// Builds and runs the send pipeline for the given <paramref name="context"/>,
    /// forwarding to the correct <see cref="IInterBusTransport"/> as the terminal step.
    /// </summary>
    public Task ExecuteAsync(SendContext context, CancellationToken cancellationToken = default)
    {
        var pipeline = TerminalDeliverDelegate;
        foreach (var middleware in _middlewares)
        {
            var current = middleware;
            var next = pipeline;
            pipeline = (ctx, ct) => current.InvokeAsync(ctx, next, ct);
        }

        return pipeline(context, cancellationToken);

        Task TerminalDeliverDelegate(SendContext ctx, CancellationToken ct)
        {
            var transport = transportFactory.Resolve(ctx.Destination);
            var sendContext = new TransportSendContext(ctx.Destination, ctx.Payload, ctx.Headers, ctx.Delay);
            return transport.SendAsync(sendContext, ct);
        }
    }
}
