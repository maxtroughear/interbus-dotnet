using InterBus.Middleware;

namespace InterBus.Internal;

/// <summary>
/// Consumer-scoped <see cref="IMessagePublisher"/> implementation. Routes publish and send
/// operations through the consumer-specific <see cref="IPublishMiddleware"/> and
/// <see cref="ISendMiddleware"/> pipelines registered for <paramref name="consumerType"/>.
/// Used by <see cref="ConsumeContext"/> so that middleware registered via
/// <c>UsePublishMiddleware</c> and <c>UseSendMiddleware</c> on a consumer builder is
/// applied when the consumer publishes or sends a message from within its
/// <c>Consume</c> method.
/// </summary>
internal sealed class ConsumerScopedMessagePublisher(
    InterBusOptions options,
    IMiddlewareFactory middlewareFactory,
    IInterBusTransportFactory transportFactory,
    IMessageSerializer serializer,
    Type consumerType) : MessagePublisherBase(options)
{
    protected override Func<PublishContext<TMessage>, CancellationToken, Task> BuildPublishPipeline<TMessage>()
    {
        // Terminal publish step: serialize and forward through the consumer-scoped send pipeline.
        Func<PublishContext<TMessage>, CancellationToken, Task> pipeline = (ctx, ct) =>
        {
            var payload = serializer.Serialize(ctx.Message);
            var sendContext = new SendContext(ctx.Destination, payload, ctx.Headers, ctx.Delay);
            return ExecuteSendPipelineAsync(sendContext, ct);
        };

        var middlewares = middlewareFactory.Resolve<IPublishMiddleware>(consumerType);
        foreach (var middleware in middlewares)
        {
            var next = pipeline;
            pipeline = (ctx, ct) => middleware.InvokeAsync(ctx, next, ct);
        }

        return pipeline;
    }

    private Task ExecuteSendPipelineAsync(SendContext context, CancellationToken ct)
    {
        Func<SendContext, CancellationToken, Task> pipeline = (ctx, token) =>
        {
            var transport = transportFactory.Resolve(ctx.Destination);
            var transportContext = new TransportSendContext(ctx.Destination, ctx.Payload, ctx.Headers, ctx.Delay);
            return transport.SendAsync(transportContext, token);
        };

        var middlewares = middlewareFactory.Resolve<ISendMiddleware>(consumerType);
        foreach (var middleware in middlewares)
        {
            var next = pipeline;
            pipeline = (ctx, token) => middleware.InvokeAsync(ctx, next, token);
        }

        return pipeline(context, ct);
    }
}
