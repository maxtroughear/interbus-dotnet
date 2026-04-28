using Microsoft.Extensions.DependencyInjection;

namespace InterBus.Transporting;

/// <summary>
/// Default <see cref="ITransportFactory"/> implementation. Resolves an
/// <see cref="ITransport"/> by looking up a keyed service registered under the
/// destination URI's scheme. Falls back to the non-keyed <see cref="ITransport"/>
/// registration (if any) so that test doubles registered without a key continue to work.
/// </summary>
internal sealed class TransportFactory(IServiceProvider serviceProvider) : ITransportFactory
{
    /// <inheritdoc/>
    public ITransport Resolve(Uri destination) =>
        serviceProvider.GetKeyedService<ITransport>(destination.Scheme)
        ?? throw new InvalidOperationException(
            $"No IInterBusTransport registered for destination scheme '{destination.Scheme}'. " +
            "Register a transport extension (e.g. AddInMemoryTransport()) or register a keyed " +
            "IInterBusTransport for the scheme.");
}
