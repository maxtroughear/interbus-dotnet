namespace InterBus.Middleware;

/// <summary>
/// A step in the schedule middleware pipeline.
/// Implementations are resolved from the DI container for each
/// <see cref="IMessageScheduler"/> scope and executed in registration order
/// (first registered = outermost).
/// </summary>
/// <remarks>
/// Unlike <see cref="IPublishMiddleware"/>, the schedule middleware operates on an
/// already-serialized payload via <see cref="ScheduleContext"/>. This makes it suitable
/// for use cases such as the Claim Check pattern, where large payloads are offloaded to
/// external storage before the scheduling record is created.
/// Register via <see cref="IInterBusBuilder.UseScheduleMiddleware{TMiddleware}"/>.
/// </remarks>
public interface IScheduleMiddleware : IMiddleware
{
    /// <summary>
    /// Execute this middleware step.
    /// Call <paramref name="next"/> to pass control to the next step in the pipeline.
    /// </summary>
    Task InvokeAsync(ScheduleContext context, Func<ScheduleContext, CancellationToken, Task> next,
        CancellationToken cancellationToken = default);
}
