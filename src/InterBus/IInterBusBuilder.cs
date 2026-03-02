using System.Diagnostics.CodeAnalysis;
using InterBus.Middleware;
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
    /// here before calling <see cref="AddConsumer{TConsumer}"/> to enable AOT-safe or custom serialization.
    /// </summary>
    InterBusOptions Options { get; }

    /// <summary>
    /// Configure an explicit topic name for message type <typeparamref name="T"/>. The name is used
    /// by both consumer registration and <see cref="IMessagePublisher.Publish{T}"/>, overriding the
    /// convention-based name derived from the type's fully-qualified name.
    /// Call this before <see cref="AddConsumer{TConsumer}"/> for the same message type.
    /// </summary>
    IInterBusBuilder ConfigureTopic<T>(string topicName) where T : class;

    /// <summary>
    /// Register a consumer with a source-generated dispatch delegate.
    /// This is the only registration path; call it via the source-generated
    /// <c>AddConsumers()</c> extension method which is produced at compile time.
    /// <para>
    /// The <paramref name="messageTypes"/> array is used to resolve topic names (respecting any
    /// <see cref="ConfigureTopic{T}"/> overrides) and to subscribe the consumer's queue to each topic.
    /// The <paramref name="dispatch"/> delegate reads the <see cref="InterBusHeaders.MessageType"/>
    /// header and routes to the appropriate <c>Consume</c> overload — no runtime reflection.
    /// </para>
    /// </summary>
    IInterBusBuilder AddConsumer<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TConsumer>(
        Type[] messageTypes,
        Func<IServiceProvider, string, IConsumeContext, CancellationToken, Task> dispatch)
        where TConsumer : class, IConsumer;
    
    /// <summary>
    /// Register a consumer with a source-generated dispatch delegate.
    /// This is the only registration path; call it via the source-generated
    /// <c>AddConsumers()</c> extension method which is produced at compile time.
    /// <para>
    /// The <paramref name="messageTypes"/> array is used to resolve topic names (respecting any
    /// <see cref="ConfigureTopic{T}"/> overrides) and to subscribe the consumer's queue to each topic.
    /// The <paramref name="queueName"/> is used to override the default queue name derived from the
    /// consumer type's fully qualified name
    /// The <paramref name="dispatch"/> delegate reads the <see cref="InterBusHeaders.MessageType"/>
    /// header and routes to the appropriate <c>Consume</c> overload — no runtime reflection.
    /// </para>
    /// </summary>
    IInterBusBuilder AddConsumer<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TConsumer>(
        Type[] messageTypes,
        string queueName,
        Func<IServiceProvider, string, IConsumeContext, CancellationToken, Task> dispatch)
        where TConsumer : class, IConsumer;

    /// <summary>
    /// Add a middleware step to the publish pipeline.
    /// Steps are executed in registration order — the first registered step is outermost
    /// and runs first. The terminal step calls the configured <see cref="IInterBusTransport"/>.
    /// </summary>
    IInterBusBuilder UsePublishMiddleware<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TMiddleware>()
        where TMiddleware : class, IPublishMiddleware;

    /// <summary>
    /// Add a middleware step to the consume pipeline.
    /// Steps are executed in registration order — the first registered step is outermost
    /// and runs first. The terminal step invokes the consumer's dispatch delegate.
    /// </summary>
    /// <remarks>
    /// An exception-logging middleware is automatically registered as the first (outermost)
    /// step before any user-defined steps.
    /// </remarks>
    IInterBusBuilder UseConsumeMiddleware<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TMiddleware>()
        where TMiddleware : class, IConsumeMiddleware;
}
