using InterBus.Middleware;

namespace InterBus.Publisher;

/// <summary>
/// Bus scoped <see cref="IMessagePublisher"/> implementation. Serializes each message and routes it
/// through the configured <see cref="IPublishMiddleware"/> pipeline. The terminal pipeline step
/// resolves the correct <see cref="IInterBusTransport"/> by the destination URI scheme via
/// <see cref="IInterBusTransportFactory"/> and forwards the message.
/// Always stamps the <see cref="InterBusHeaders.MessageType"/> header so consumer dispatch
/// delegates can identify the message type without runtime reflection.
/// </summary>
internal sealed class BusMessagePublisher(InterBusOptions options, IMiddlewareFactory middlewareFactory)
    : MessagePublisherBase(options)
{
    protected override Func<PublishContext<TMessage>, CancellationToken, Task> BuildPublishPipeline<TMessage>()
    {
        Func<PublishContext<TMessage>, CancellationToken, Task> pipeline = (_, _) => Task.CompletedTask;

        var middlewares = middlewareFactory.Resolve<IPublishMiddleware>();
        foreach (var middleware in middlewares)
        {
            var next = pipeline;
            pipeline = (ctx, ct) => middleware.InvokeAsync(ctx, next, ct);
        }

        return pipeline;
    }
}
