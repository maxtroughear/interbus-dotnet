using Microsoft.Extensions.DependencyInjection;

namespace InterBus.Dispatching;

public class Dispatcher(IServiceScopeFactory scopeFactory)
{
    public async Task DispatchAsync(DispatchContext context, CancellationToken cancellationToken)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var serviceProvider = scope.ServiceProvider;
        var executor = serviceProvider.GetRequiredService<DispatchPipelineExecutor>();
        await executor.ExecuteAsync(context, cancellationToken);
    }
}
