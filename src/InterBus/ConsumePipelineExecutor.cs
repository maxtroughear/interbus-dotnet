namespace InterBus;

/// <summary>
/// Executes the consume middleware pipeline for a single message dispatch.
/// Resolved from the per-message DI scope created by the message pump so that
/// each middleware is also resolved from the correct scope.
/// Transport packages should resolve this service from the message scope and call
/// <see cref="ExecuteAsync"/> instead of invoking the consumer dispatch delegate directly.
/// </summary>
public sealed class ConsumePipelineExecutor
{
    private readonly IEnumerable<IConsumeMiddleware> _middlewares;
    private readonly IServiceProvider _serviceProvider;

    /// <summary>
    /// Initializes the executor. Both parameters are resolved from the per-message scope.
    /// </summary>
    public ConsumePipelineExecutor(
        IEnumerable<IConsumeMiddleware> middlewares,
        IServiceProvider serviceProvider)
    {
        _middlewares = middlewares;
        _serviceProvider = serviceProvider;
    }

    /// <summary>
    /// Builds and runs the consume pipeline for the given <paramref name="descriptor"/>.
    /// </summary>
    public Task ExecuteAsync(
        ConsumerDescriptor descriptor,
        string payload,
        MessageHeaders headers,
        CancellationToken cancellationToken = default)
    {
        var context = new ConsumePipelineContext(descriptor, payload, headers);

        ConsumeDelegate terminal = (ctx, ct) =>
            ctx.Descriptor.Dispatch(_serviceProvider, ctx.Payload, ctx, ct);

        // Build pipeline: first registered middleware is outermost.
        // Iterate in reverse so each new step wraps the already-built inner chain.
        var pipeline = terminal;
        foreach (var middleware in _middlewares.Reverse())
        {
            var current = middleware;
            var next = pipeline;
            pipeline = (ctx, ct) => current.InvokeAsync(ctx, next, ct);
        }

        return pipeline(context, cancellationToken);
    }
}
