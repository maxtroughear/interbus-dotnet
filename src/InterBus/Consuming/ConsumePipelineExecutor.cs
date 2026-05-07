using System.Diagnostics;
using InterBus.Middleware;
using InterBus.Telemetry;
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
    IInterBusDiagnostics diagnostics,
    IServiceProvider serviceProvider)
{
    private InterBusMetrics Metrics => serviceProvider.GetRequiredService<InterBusMetrics>();

    /// <summary>
    /// Builds and runs the consume pipeline for the given <paramref name="context"/> and
    /// <paramref name="finalDelegate"/>
    /// </summary>
    public async Task ExecuteAsync<T>(
        ConsumeContext<T> context,
        Func<T, IConsumeContext, CancellationToken, Task> finalDelegate,
        CancellationToken cancellationToken = default) where T : class
    {
        contextAccessor.ConsumeContext = context;

        var pipeline = ConsumerDelegate;

        var middlewares = middlewareFactory.Resolve<IConsumeMiddleware>(context.Descriptor.ConsumerType);

        foreach (var middleware in middlewares)
        {
            var next = pipeline;
            pipeline = (ctx, ct) => middleware.InvokeAsync(ctx, next, ct);
        }

        var messageType = context.Headers.GetValueOrDefault(InterBusHeaders.MessageType);
        var consumerName = context.Descriptor.ConsumerType.Name;

        using var activity = StartActivity(context, consumerName, messageType);

        try
        {
            await pipeline(context, cancellationToken);

            activity?.SetStatus(ActivityStatusCode.Ok);
            Metrics.MessagesConsumed.Add(1,
                new TagList
                {
                    { "messaging.system", "interbus" },
                    { "messaging.consumer.name", consumerName },
                    { "messaging.destination.name", context.Descriptor.QueueName },
                });
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            Metrics.MessagesFailed.Add(1,
                new TagList
                {
                    { "messaging.system", "interbus" },
                    { "messaging.consumer.name", consumerName },
                    { "messaging.destination.name", context.Descriptor.QueueName },
                });
            throw;
        }

        async Task ConsumerDelegate(ConsumeContext<T> ctx, CancellationToken ct)
        {
            await finalDelegate(ctx.Message, ctx, ct);
        }
    }

    private Activity? StartActivity<T>(ConsumeContext<T> context, string consumerName, string? messageType)
        where T : class
    {
        ActivityContext parentContext = default;
        var hasParent = context.Headers.TryGetValue("traceparent", out var traceparent)
            && ActivityContext.TryParse(traceparent, context.Headers.GetValueOrDefault("tracestate"),
                isRemote: true, out parentContext);

        var activityName = $"{consumerName} process";
        var activity = hasParent
            ? diagnostics.ActivitySource.StartActivity(activityName, ActivityKind.Consumer, parentContext)
            : diagnostics.ActivitySource.StartActivity(activityName, ActivityKind.Consumer);

        if (activity is null)
            return null;

        activity.SetTag("messaging.system", "interbus");
        activity.SetTag("messaging.operation.type", "process");
        activity.SetTag("messaging.destination.name", context.Descriptor.QueueName);
        activity.SetTag("messaging.consumer.name", consumerName);

        if (messageType is not null)
            activity.SetTag("messaging.message.type", messageType);

        if (context.Headers.TryGetValue(InterBusHeaders.MessageId, out var messageId))
            activity.SetTag("messaging.message.id", messageId);

        if (context.CorrelationId is not null)
            activity.SetTag("messaging.message.correlation_id", context.CorrelationId);

        return activity;
    }
}
