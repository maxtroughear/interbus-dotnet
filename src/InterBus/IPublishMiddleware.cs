namespace InterBus;

/// <summary>
/// A step in the publish middleware pipeline.
/// Implementations are resolved from the DI container for each
/// <see cref="IMessagePublisher"/> scope and executed in registration order
/// (first registered = outermost).
/// </summary>
/// <remarks>
/// Register via <see cref="IInterBusBuilder.UsePublishMiddleware{TMiddleware}"/>.
/// </remarks>
public interface IPublishMiddleware
{
    /// <summary>
    /// Execute this middleware step.
    /// Call <paramref name="next"/> to pass control to the next step in the pipeline.
    /// </summary>
    Task InvokeAsync(PublishContext context, PublishDelegate next, CancellationToken cancellationToken = default);
}

/// <summary>
/// Delegate representing a step — or the remainder — of the publish pipeline.
/// </summary>
public delegate Task PublishDelegate(PublishContext context, CancellationToken cancellationToken = default);
