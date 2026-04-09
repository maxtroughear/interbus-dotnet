namespace InterBus.Middleware.Internal;

internal class PublishToSendPipeline(SendPipelineExecutor sendPipelineExecutor, IMessageSerializer serializer)
    : IPublishMiddleware
{
    public Task InvokeAsync<T>(PublishContext<T> ctx, Func<PublishContext<T>, CancellationToken, Task> next,
        CancellationToken ct = default) where T : class
    {
        var payload = serializer.Serialize(ctx.Message);
        var sendContext = new SendContext(ctx.Destination, payload, ctx.Headers, ctx.Delay);
        return sendPipelineExecutor.ExecuteAsync(sendContext, ct);
    }
}
