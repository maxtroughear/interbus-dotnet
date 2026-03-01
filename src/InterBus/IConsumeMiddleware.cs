namespace InterBus;

/// <summary>
/// A step in the consume middleware pipeline.
/// Implementations are resolved from the DI container for each message dispatch scope
/// and executed in registration order (first registered = outermost).
/// </summary>
/// <remarks>
/// Register via <see cref="IInterBusBuilder.UseConsumeMiddleware{TMiddleware}"/>.
/// </remarks>
public interface IConsumeMiddleware
{
    /// <summary>
    /// Execute this middleware step.
    /// Call <paramref name="next"/> to pass control to the next step in the pipeline.
    /// </summary>
    Task InvokeAsync(ConsumePipelineContext context, ConsumeDelegate next, CancellationToken cancellationToken = default);
}

/// <summary>
/// Delegate representing a step — or the remainder — of the consume pipeline.
/// </summary>
public delegate Task ConsumeDelegate(ConsumePipelineContext context, CancellationToken cancellationToken = default);
