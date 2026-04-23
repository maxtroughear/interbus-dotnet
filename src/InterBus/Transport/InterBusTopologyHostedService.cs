using Microsoft.Extensions.Hosting;

namespace InterBus.Transport;

/// <summary>
/// Executes all registered transport topology builders once during host startup.
/// </summary>
public sealed class InterBusTopologyHostedService(
    IEnumerable<ITopologyBuilder> topologyBuilders,
    InterBusOptions busOptions)
    : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (busOptions.Consumers.Count == 0)
            return;

        foreach (var topologyBuilder in topologyBuilders)
            await topologyBuilder.BuildTopologyAsync(cancellationToken);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
