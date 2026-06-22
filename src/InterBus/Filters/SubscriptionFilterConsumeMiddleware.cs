using InterBus.Consuming;

namespace InterBus.Filters;

/// <summary>
/// Skips consumer execution when the registered subscription filter does not match the
/// deserialized message.
/// </summary>
public sealed class SubscriptionFilterConsumeMiddleware : IConsumeMiddleware
{
    public Task InvokeAsync<T>(
        ConsumeContext<T> context,
        Func<ConsumeContext<T>, CancellationToken, Task> next,
        CancellationToken cancellationToken = default) where T : class
    {
        if (context.Descriptor.SubscriptionFilters.TryGetValue(typeof(T), out var filter) &&
            !filter.Invoke(context.Message))
        {
            context.Result = Dispatching.DispatchResult.Success;
            return Task.CompletedTask;
        }

        return next(context, cancellationToken);
    }
}
