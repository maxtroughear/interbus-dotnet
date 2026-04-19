using InterBus.Consumer;
using InterBus.Middleware;
using InterBus.Send;

namespace InterBus.Publisher;

public sealed class PublishPipelineExecutor(
    SendPipelineExecutor sendPipeline,
    IMessageSerializer serializer,
    IMiddlewareFactory middlewareFactory,
    ConsumeContextAccessor contextAccessor)
{
    public Task ExecuteAsync<TMessage>(PublishContext<TMessage> context, CancellationToken cancellationToken = default) where TMessage : class
    {
        var pipeline = SendDelegate;

        var key = contextAccessor.ConsumeContext?.Descriptor.ConsumerType;
        
        var middlewares = middlewareFactory.Resolve<IPublishMiddleware>(key);
        foreach (var middleware in middlewares)
        {
            var next = pipeline;
            pipeline = (ctx, ct) => middleware.InvokeAsync(ctx, next, ct);
        }
        
        return pipeline(context, cancellationToken);
        
        Task SendDelegate(PublishContext<TMessage> ctx, CancellationToken ct)
        {
            var payload = serializer.Serialize(ctx.Message);
            var sendContext = new SendContext(ctx.Destination, payload, ctx.Headers, ctx.Delay);
            return sendPipeline.ExecuteAsync(sendContext, cancellationToken: ct);
        }
    }
}
