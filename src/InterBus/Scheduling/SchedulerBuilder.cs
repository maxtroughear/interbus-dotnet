using Microsoft.Extensions.DependencyInjection;

namespace InterBus.Scheduling;

public class SchedulerBuilder(IInterBusBuilder bus, IServiceCollection services) : ISchedulerBuilder
{
    public IInterBusBuilder Bus => bus;
    public IServiceCollection Services => services;
}
