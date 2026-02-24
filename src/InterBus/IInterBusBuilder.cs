using Microsoft.Extensions.DependencyInjection;

namespace InterBus;

/// <summary>
/// Fluent builder for configuring InterBus — consumers, transport, and options.
/// Obtained from <c>services.AddInterBus(builder => { ... })</c>.
/// </summary>
public interface IInterBusBuilder
{
    /// <summary>The underlying service collection, available for transport extension methods to register additional services.</summary>
    IServiceCollection Services { get; }

    /// <summary>
    /// Register a consumer. The consumer's <see cref="IConsumer{T}"/> interfaces are discovered automatically
    /// (startup-time reflection only). The consumer type is registered as scoped in DI.
    /// </summary>
    IInterBusBuilder AddConsumer<TConsumer>() where TConsumer : class, IConsumer;
}
