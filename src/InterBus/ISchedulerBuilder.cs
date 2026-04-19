using Microsoft.Extensions.DependencyInjection;

namespace InterBus;

public interface ISchedulerBuilder
{
    /// <summary>The underlying service collection, available for transport extension methods to register additional services.</summary>
    IServiceCollection Services { get; }
    
    IInterBusBuilder Bus { get; }
}
