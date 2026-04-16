using InterBus.Middleware;

namespace InterBus.Scheduler;

/// <summary>
/// Abstract base for consumer-scoped <see cref="IMessageScheduler"/> implementations.
/// Handles header stamping (including causation-tracking headers propagated from the
/// triggering consume context) and the schedule middleware pipeline. Subclasses provide
/// the terminal delivery step by implementing <see cref="BuildTerminal"/>.
/// </summary>
internal abstract class ConsumerScopedMessageSchedulerBase(
    InterBusOptions options,
    IMiddlewareFactory middlewareFactory,
    IMessageSerializer serializer,
    Type consumerType,
    IConsumeContextAccessor contextAccessor)
    : MessageSchedulerBase(options, serializer)
{
    /// <inheritdoc/>
    protected override MessageHeaders Stamp<T>() where T : class
    {
        var headers = base.Stamp<T>();
        var consumeHeaders = contextAccessor.ConsumeContext?.Headers;

        if (consumeHeaders?.TryGetValue(InterBusHeaders.CorrelationId, out var correlationId) == true)
            headers[InterBusHeaders.CorrelationId] = correlationId;

        if (consumeHeaders?.TryGetValue(InterBusHeaders.MessageId, out var messageId) == true)
            headers[InterBusHeaders.CausationId] = messageId;

        return headers;
    }

    /// <inheritdoc/>
    protected override Task ScheduleInternalAsync<T>(PublishContext<T> context, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken) where T : class
    {
        var payload = Serializer.Serialize(context.Message);

        var delay = scheduledAt - DateTimeOffset.UtcNow;
        if (delay < TimeSpan.Zero) delay = TimeSpan.Zero;

        var scheduleContext = new ScheduleContext(context.Destination, payload, context.Headers, delay);
        return ExecuteSchedulePipelineAsync(scheduleContext, scheduledAt, cancellationToken);
    }

    private Task ExecuteSchedulePipelineAsync(ScheduleContext scheduleContext, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken)
    {
        var pipeline = BuildTerminal(scheduledAt);
        var middlewares = middlewareFactory.Resolve<IScheduleMiddleware>(consumerType);
        foreach (var middleware in middlewares)
        {
            var current = middleware;
            var next = pipeline;
            pipeline = (ctx, ct) => current.InvokeAsync(ctx, next, ct);
        }

        return pipeline(scheduleContext, cancellationToken);
    }

    /// <summary>
    /// Builds the terminal step of the schedule pipeline for the given
    /// <paramref name="scheduledAt"/> delivery time.
    /// </summary>
    protected abstract Func<ScheduleContext, CancellationToken, Task> BuildTerminal(DateTimeOffset scheduledAt);
}
