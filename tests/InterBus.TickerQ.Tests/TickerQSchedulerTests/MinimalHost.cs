using Microsoft.Extensions.Hosting;

namespace InterBus.TickerQ.Tests;

public sealed partial class TickerQSchedulerTests
{
    private sealed class MinimalHost(IServiceProvider services) : IHost
    {
        public IServiceProvider Services => services;

        public Task StartAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public void Dispose()
        {
        }
    }
}
