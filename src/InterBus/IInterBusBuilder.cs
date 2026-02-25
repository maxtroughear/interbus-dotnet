using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus;

/// <summary>
/// Fluent builder for configuring InterBus - consumers, transport, and options.
/// Obtained from <c>services.AddInterBus(builder => { ... })</c>.
/// </summary>
public interface IInterBusBuilder
{
    /// <summary>The underlying service collection, available for transport extension methods to register additional services.</summary>
    IServiceCollection Services { get; }

    /// <summary>
    /// The runtime options for this InterBus instance. Configure <see cref="InterBusOptions.SerializerOptions"/>
    /// here before calling <see cref="AddConsumer{TConsumer,T}"/> to enable AOT-safe or custom serialization.
    /// </summary>
    InterBusOptions Options { get; }

    /// <summary>
    /// Register a consumer for a specific message type with both type parameters supplied at compile time.
    /// No runtime reflection is performed - this overload is intended to be called by source-generated
    /// code produced by the <c>InterBus.SourceGenerator</c> package.
    /// </summary>
    IInterBusBuilder AddConsumer<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TConsumer, T>()
        where TConsumer : class, IConsumer<T>
        where T : class;
}
