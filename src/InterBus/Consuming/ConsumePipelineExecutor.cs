using InterBus.Dispatching;
using InterBus.Middleware;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.Consuming;

/// <summary>
/// Executes the consume middleware pipeline for a single message dispatch.
/// Resolved from the per-message DI scope created by the message pump so that
/// each middleware is also resolved from the correct scope.
/// Transport packages should resolve this service from the message scope and call
/// <see cref="ExecuteAsync{TConsume,TMessage}(DispatchContext, CancellationToken)"/>
/// instead of invoking the consumer dispatch delegate directly.
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
    public async Task ExecuteAsync<TConsume, TMessage>(
        DispatchContext context,
        CancellationToken cancellationToken = default)
        where TConsume : IConsume<TMessage>
        where TMessage : class
    {
        try
        {
            var message = serializer.Deserialize<TMessage>(context.Payload);
            if (message is null)
            {
                throw new InvalidOperationException(
                    $"Failed to deserialize message of type {typeof(TMessage).FullName} for consumer {typeof(TConsume).FullName}");
            }

            var consumeContext = new ConsumeContext<TMessage>(context.Descriptor, message, context.Headers)
            {
                Result = context.Result
            };

            var executor = serviceProvider.GetRequiredService<IConsumerExecutor<TConsume, TMessage>>();

            await ExecuteAsync(consumeContext, executor.ExecuteAsync, cancellationToken);
            context.Result = consumeContext.Result;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            context.Result = DispatchResult.Release(ex);
        }
    }
}
