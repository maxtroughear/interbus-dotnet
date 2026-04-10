using InterBus.Middleware;

namespace InterBus.Internal;

/// <summary>
/// Consumer-scoped <see cref="IMessageScheduler"/> for the direct scheduling path
/// (<c>AddScheduling()</c>). Forwards scheduling requests directly to the registered
/// <see cref="IMessageSchedulingProvider"/>, running consumer-keyed
/// <see cref="IScheduleMiddleware"/> steps before the provider call.
/// </summary>
internal sealed class DirectConsumerScopedMessageScheduler(
    InterBusOptions options,
    IMiddlewareFactory middlewareFactory,
    IMessageSerializer serializer,
    Type consumerType,
    MessageHeaders consumeHeaders,
    IMessageSchedulingProvider provider)
    : ConsumerScopedMessageSchedulerBase(options, middlewareFactory, serializer, consumerType, consumeHeaders)
{
    protected override Func<ScheduleContext, CancellationToken, Task> BuildTerminal(DateTimeOffset scheduledAt) =>
        (ctx, ct) => provider.ScheduleAsync(ctx.Destination, ctx.Payload, ctx.Headers, scheduledAt, ct);
}
