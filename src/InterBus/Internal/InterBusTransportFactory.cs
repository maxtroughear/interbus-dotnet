using Microsoft.Extensions.DependencyInjection;

namespace InterBus.Internal;

/// <summary>
/// Default <see cref="IInterBusTransportFactory"/> implementation. Resolves an
/// <see cref="IInterBusTransport"/> by looking up a keyed service registered under the
/// destination URI's scheme. Falls back to the non-keyed <see cref="IInterBusTransport"/>
/// registration (if any) so that test doubles registered without a key continue to work.
/// </summary>
internal sealed class InterBusTransportFactory(IServiceProvider serviceProvider) : IInterBusTransportFactory
{
    /// <inheritdoc/>
    public IInterBusTransport Resolve(Uri destination) =>
        serviceProvider.GetKeyedService<IInterBusTransport>(destination.Scheme)
        ?? serviceProvider.GetService<IInterBusTransport>()
        ?? throw new InvalidOperationException(
            $"No IInterBusTransport registered for destination scheme '{destination.Scheme}'. " +
            "Register a transport extension (e.g. AddInMemoryTransport()) or register a keyed " +
            "IInterBusTransport for the scheme.");
}
