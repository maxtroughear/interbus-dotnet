using InterBus.Dispatching;
using InterBus.Middleware;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.Consuming;

/// <summary>
/// Executes the consume middleware pipeline for a single message dispatch.
/// Resolved from the per-message DI scope created by the message pump so that
/// each middleware is also resolved from the correct scope.
/// Transport packages should resolve this service from the message scope and call
/// <see cref="ExecuteAsync"/> instead of invoking the consumer dispatch delegate directly.
/// </summary>
public sealed class ConsumePipelineExecutor(
    IMiddlewareFactory middlewareFactory,
    ConsumeContextAccessor contextAccessor,
    IMessageSerializer serializer,
    IServiceProvider serviceProvider)
{
    /// <summary>
    /// Builds and runs the consume pipeline for the given <paramref name="context"/> and
    /// <paramref name="finalDelegate"/>
    /// </summary>
    public Task ExecuteAsync<T>(
        ConsumeContext<T> context,
        Func<ConsumeContext<T>, CancellationToken, Task> finalDelegate,
        CancellationToken cancellationToken = default) where T : class
    {
        contextAccessor.ConsumeContext = context;

        var pipeline = finalDelegate;

        var middlewares = middlewareFactory.Resolve<IConsumeMiddleware>(context.Descriptor.ConsumerType);

        foreach (var middleware in middlewares)
        {
            var next = pipeline;
            pipeline = (ctx, ct) => middleware.InvokeAsync(ctx, next, ct);
        }

        return pipeline(context, cancellationToken);
    }

    /// <summary>
    /// Builds and runs the consume pipeline for the given <paramref name="context"/>
    /// </summary>
    public Task ExecuteAsync<TConsumer, TMessage>(
        DispatchContext context,
        CancellationToken cancellationToken = default)
        where TMessage : class
    {
        var message = serializer.Deserialize<TMessage>(context.Payload);
        if (message is null)
        {
            throw new InvalidOperationException(
                $"Failed to deserialize message of type {typeof(TMessage).FullName} for consumer {typeof(TConsumer).FullName}");
        }

        var consumeContext = new ConsumeContext<TMessage>(context.Descriptor, message, context.Headers);
                
        var executor = serviceProvider.GetRequiredService<IConsumerExecutor<TConsumer, TMessage>>();
                
        return ExecuteAsync(consumeContext, executor.ExecuteAsync, cancellationToken);
    }
}
