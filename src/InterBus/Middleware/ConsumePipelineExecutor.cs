using InterBus.Consumer;
using InterBus.Publisher;
using InterBus.Scheduler;

namespace InterBus.Middleware;

/// <summary>
/// Executes the consume middleware pipeline for a single message dispatch.
/// Resolved from the per-message DI scope created by the message pump so that
/// each middleware is also resolved from the correct scope.
/// Transport packages should resolve this service from the message scope and call
/// <see cref="ExecuteAsync"/> instead of invoking the consumer dispatch delegate directly.
/// </summary>
public sealed class ConsumePipelineExecutor(
    IMiddlewareFactory middlewareFactory,
    InterBusOptions options,
    IInterBusTransportFactory transportFactory,
    IMessageSerializer serializer,
    IConsumeContextAccessor contextAccessor,
    IConsumerSchedulerFactory? schedulerFactory = null,
    IMessageSchedulingProvider? schedulingProvider = null)
{
    /// <summary>
    /// Builds and runs the consume pipeline for the given <paramref name="context"/> and
    /// <paramref name="finalDelegate"/>
    /// </summary>
    public Task ExecuteAsync<T>(
        ConsumeContext<T> context,
        Func<T, IConsumeContext, CancellationToken, Task> finalDelegate,
        CancellationToken cancellationToken = default) where T : class
    {
        var pipeline = TerminalConsumeDelegate;

        var middlewares = middlewareFactory.Resolve<IConsumeMiddleware>(context.Descriptor.ConsumerType);

        foreach (var middleware in middlewares)
        {
            var next = pipeline;
            pipeline = (ctx, ct) => middleware.InvokeAsync(ctx, next, ct);
        }

        return pipeline(context, cancellationToken);

        async Task TerminalConsumeDelegate(ConsumeContext<T> ctx, CancellationToken ct)
        {
            var publisher = new ConsumerScopedMessagePublisher(
                options, middlewareFactory, transportFactory, serializer, ctx.Descriptor.ConsumerType,
                contextAccessor);

            var scheduler = schedulerFactory?.CreateForConsumer(
                middlewareFactory, serializer, ctx.Descriptor.ConsumerType, publisher, schedulingProvider,
                contextAccessor);

            // Create the ConsumeContext and immediately register it on the accessor.
            // The publisher and scheduler read from the accessor only when the consumer calls
            // Publish/Send/Schedule (inside finalDelegate), so the ordering is safe.
            // Clear the accessor after dispatch (via finally) so downstream code in the same
            // scope cannot observe a stale context once the consumer has finished.
            var consumeContext = new ConsumeContext(ctx.Headers, publisher, scheduler);
            var previous = contextAccessor.ConsumeContext;
            contextAccessor.ConsumeContext = consumeContext;
            try
            {
                await finalDelegate(ctx.Message, consumeContext, ct);
            }
            finally
            {
                contextAccessor.ConsumeContext = previous;
            }
        }
    }
}
