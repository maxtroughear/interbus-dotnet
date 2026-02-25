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
    /// Configure an explicit topic name for message type <typeparamref name="T"/>. The name is used
    /// by both consumer registration and <see cref="IMessagePublisher.Publish{T}"/>, overriding the
    /// convention-based name derived from the type's fully-qualified name.
    /// Call this before <see cref="AddConsumer{TConsumer,T}"/> for the same message type.
    /// </summary>
    IInterBusBuilder ConfigureTopic<T>(string topicName) where T : class;

    /// <summary>
    /// Register a consumer with a source-generated dispatch delegate that handles all of the
    /// consumer's message types without runtime reflection. This is the primary registration path
    /// and is called automatically by <c>AddGeneratedConsumers()</c>.
    /// <para>
    /// The <paramref name="messageTypes"/> array is used to resolve topic names (respecting any
    /// <see cref="ConfigureTopic{T}"/> overrides), and to subscribe the consumer's queue to each topic.
    /// The <paramref name="dispatch"/> delegate reads the <see cref="InterBusHeaders.MessageType"/>
    /// header and routes to the appropriate <c>Consume</c> overload.
    /// </para>
    /// </summary>
    IInterBusBuilder AddConsumer<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TConsumer>(
        Type[] messageTypes,
        Func<IServiceProvider, string, MessageHeaders, CancellationToken, Task> dispatch)
        where TConsumer : class, IConsumer;

    /// <summary>
    /// Register a consumer for a single message type. Suitable for manual registration in tests
    /// or when source generation is not used. For consumers that handle multiple message types,
    /// call this method once per type; the framework merges them into a single queue automatically.
    /// </summary>
    IInterBusBuilder AddConsumer<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TConsumer, T>()
        where TConsumer : class, IConsumer<T>
        where T : class;
}
