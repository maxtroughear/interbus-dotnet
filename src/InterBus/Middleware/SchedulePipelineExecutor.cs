namespace InterBus.Middleware;

/// <summary>
/// Executes the schedule middleware pipeline for a single scheduled-message delivery.
/// Resolved from the per-delivery DI scope so that each middleware is also resolved from
/// the correct scope.
/// Scheduling provider packages (e.g. <c>InterBus.TickerQ</c>) should resolve this service
/// and call <see cref="ExecuteAsync"/> instead of forwarding to the transport directly,
/// so that <see cref="IScheduleMiddleware"/> steps are executed for every delivery.
/// </summary>
public sealed class SchedulePipelineExecutor(
    IEnumerable<IScheduleMiddleware> middlewares,
    IInterBusTransportFactory transportFactory)
{
    private readonly IList<IScheduleMiddleware> _middlewares = middlewares.Reverse().ToList();

    /// <summary>
    /// Builds and runs the schedule pipeline for the given <paramref name="context"/>,
    /// forwarding to the correct <see cref="IInterBusTransport"/> as the terminal step.
    /// </summary>
    public Task ExecuteAsync(ScheduleContext context, CancellationToken cancellationToken = default)
    {
        var pipeline = TerminalDeliverDelegate;
        foreach (var middleware in _middlewares)
        {
            var current = middleware;
            var next = pipeline;
            pipeline = (ctx, ct) => current.InvokeAsync(ctx, next, ct);
        }

        return pipeline(context, cancellationToken);

        Task TerminalDeliverDelegate(ScheduleContext ctx, CancellationToken ct)
        {
            var transport = transportFactory.Resolve(ctx.Destination);
            TimeSpan? delay = null;
            if (ctx.ScheduledAt is { } scheduledAt)
            {
                delay = scheduledAt - DateTimeOffset.UtcNow;
                if (delay < TimeSpan.Zero)
                    delay = TimeSpan.Zero;
            }
            var sendContext = new TransportSendContext(ctx.Destination, ctx.Payload, ctx.Headers, delay);
            return transport.SendAsync(sendContext, ct);
        }
    }
}
