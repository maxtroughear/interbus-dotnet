using InterBus.Consuming;

namespace InterBus.Filters;

public class FilterConsumeMiddleware<TMessage>(Func<TMessage, bool> filter) : IConsumeMiddleware
    where TMessage : class
{
    private readonly Func<object, bool> _filter = message => filter((TMessage)message);
    
    public Task InvokeAsync<T>(ConsumeContext<T> context, Func<ConsumeContext<T>, CancellationToken, Task> next,
        CancellationToken cancellationToken = default) where T : class
    {
        if (typeof(TMessage) != typeof(T))
        {
            return next(context, cancellationToken);
        }
        
        return _filter.Invoke(context.Message) ? next(context, cancellationToken) : Task.CompletedTask;
    }
}
