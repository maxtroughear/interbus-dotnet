using InterBus.Middleware;

namespace InterBus.Consuming;

/// <summary>
/// A step in the consume middleware pipeline.
/// Implementations are resolved from the DI container for each message dispatch scope
/// and executed in registration order (first registered = outermost).
/// </summary>
/// <remarks>
/// Register via <see cref="ConsumerBuilder.UseConsumeMiddleware{TMiddleware}"/>.
/// </remarks>
public interface IConsumeMiddleware : IMiddleware
{
    /// <summary>
    /// Execute this middleware step.
    /// Call <paramref name="next"/> to pass control to the next step in the pipeline.
    /// </summary>
    Task InvokeAsync<T>(ConsumeContext<T> context, Func<ConsumeContext<T>, CancellationToken, Task> next,
        CancellationToken cancellationToken = default) where T : class;
}
