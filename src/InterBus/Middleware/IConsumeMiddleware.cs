namespace InterBus.Middleware;

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
    Task InvokeAsync(ConsumeContext context, Func<ConsumeContext, CancellationToken, Task> next,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Delegate representing a step — or the remainder — of the consume pipeline.
/// </summary>
public delegate Task ConsumeDelegate(ConsumeContext context, CancellationToken cancellationToken = default);

