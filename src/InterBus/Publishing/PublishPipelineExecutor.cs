using System.Diagnostics;
using InterBus.Consuming;
using InterBus.Middleware;
using InterBus.Sending;
using InterBus.Telemetry;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.Publishing;

public sealed class PublishPipelineExecutor(
    SendPipelineExecutor sendPipeline,
    IMessageSerializer serializer,
    IMiddlewareFactory middlewareFactory,
    ConsumeContextAccessor contextAccessor,
    IInterBusDiagnostics diagnostics,
    IServiceProvider serviceProvider)
{
    private InterBusMetrics Metrics => serviceProvider.GetRequiredService<InterBusMetrics>();

    public async Task ExecuteAsync<TMessage>(PublishContext<TMessage> context, CancellationToken cancellationToken = default)
        where TMessage : class
    {
        var messageType = context.Headers.GetValueOrDefault(InterBusHeaders.MessageType);
        var destinationName = RoutingConventions.GetDestinationName(context.Destination);

        using var activity = StartActivity(context, messageType, destinationName);

        // Propagate trace context into outgoing message headers so consumers can link spans.
        if (activity is not null)
        {
            if (activity.Id is not null)
                context.Headers["traceparent"] = activity.Id;
            if (!string.IsNullOrEmpty(activity.TraceStateString))
                context.Headers["tracestate"] = activity.TraceStateString;
        }

        var pipeline = SendDelegate;

        var key = contextAccessor.ConsumeContext?.Descriptor.ConsumerType;

        var middlewares = middlewareFactory.Resolve<IPublishMiddleware>(key);
        foreach (var middleware in middlewares)
        {
            var next = pipeline;
            pipeline = (ctx, ct) => middleware.InvokeAsync(ctx, next, ct);
        }

        await pipeline(context, cancellationToken);

        Metrics.MessagesPublished.Add(1,
            new TagList
            {
                { "messaging.system", "interbus" },
                { "messaging.destination.name", destinationName },
            });

        Task SendDelegate(PublishContext<TMessage> ctx, CancellationToken ct)
        {
            var payload = serializer.Serialize(ctx.Message);
            var sendContext = new SendContext(ctx.Destination, payload, ctx.Headers, ctx.Delay);
            return sendPipeline.ExecuteAsync(sendContext, cancellationToken: ct);
        }
    }

    private Activity? StartActivity<TMessage>(
        PublishContext<TMessage> context,
        string? messageType,
        string destinationName) where TMessage : class
    {
        var activityName = messageType is not null ? $"{messageType} publish" : "publish";
        var activity = diagnostics.ActivitySource.StartActivity(activityName, ActivityKind.Producer);

        if (activity is null)
            return null;

        activity.SetTag("messaging.system", "interbus");
        activity.SetTag("messaging.operation.type", "publish");
        activity.SetTag("messaging.destination.name", destinationName);

        if (messageType is not null)
            activity.SetTag("messaging.message.type", messageType);

        if (context.Headers.TryGetValue(InterBusHeaders.MessageId, out var messageId))
            activity.SetTag("messaging.message.id", messageId);

        if (context.Headers.TryGetValue(InterBusHeaders.CorrelationId, out var correlationId))
            activity.SetTag("messaging.message.correlation_id", correlationId);

        return activity;
    }
}
