namespace InterBus.Middleware;

/// <summary>
/// A step in the publish middleware pipeline.
/// Implementations are resolved from the DI container for each
/// <see cref="IMessagePublisher"/> scope and executed in registration order
/// (first registered = outermost).
/// </summary>
/// <remarks>
/// Register via <see cref="IInterBusBuilder.UsePublishMiddleware{TMiddleware}"/>.
/// </remarks>
public interface IPublishMiddleware : IMiddleware
{
    /// <summary>
    /// Execute this middleware step.
    /// Call <paramref name="next"/> to pass control to the next step in the pipeline.
    /// </summary>
    Task InvokeAsync<T>(PublishContext<T> context, Func<PublishContext<T>, CancellationToken, Task> next,
        CancellationToken cancellationToken = default) where T : class;
}
