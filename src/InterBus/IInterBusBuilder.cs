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
    /// The runtime options for this InterBus instance. Configure topic names and consumer
    /// registration here before the host is built.
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
    /// header and routes to the appropriate <c>Consume</c> overload - no runtime reflection.
    /// </para>
    /// </summary>
    IInterBusBuilder AddConsumer<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TConsumer>(
        Type[] messageTypes,
        Func<DispatchContext, IServiceProvider, CancellationToken, Task> dispatch)
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
    /// header and routes to the appropriate <c>Consume</c> overload - no runtime reflection.
    /// </para>
    /// </summary>
    IInterBusBuilder AddConsumer<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TConsumer>(
        Type[] messageTypes,
        string queueName,
        Func<DispatchContext, IServiceProvider, CancellationToken, Task> dispatch)
        where TConsumer : class, IConsumer;

    /// <summary>
    /// Register a custom <see cref="IMessageSerializer"/> implementation.
    /// The type is registered as a singleton in the DI container, replacing the default
    /// <see cref="SystemTextJsonMessageSerializer"/>.
    /// </summary>
    /// <typeparam name="TSerializer">The serialiser type to register.</typeparam>
    IInterBusBuilder UseSerializer<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TSerializer>()
        where TSerializer : class, IMessageSerializer;

    /// <summary>
    /// Add a middleware step to the publish pipeline.
    /// Steps are executed in registration order - the first registered step is outermost
    /// and runs first. The terminal step calls the configured <see cref="IInterBusTransport"/>.
    /// </summary>
    IInterBusBuilder UsePublishMiddleware<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TMiddleware>()
        where TMiddleware : class, IPublishMiddleware;

    /// <summary>
    /// Add a middleware step to the consume pipeline.
    /// Steps are executed in registration order - the first registered step is outermost
    /// and runs first. The terminal step invokes the consumer's dispatch delegate.
    /// </summary>
    /// <remarks>
    /// An exception-logging middleware is automatically registered as the first (outermost)
    /// step before any user-defined steps.
    /// </remarks>
    IInterBusBuilder UseConsumeMiddleware<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TMiddleware>()
        where TMiddleware : class, IConsumeMiddleware;

    /// <summary>
    /// Add a middleware step to the schedule pipeline.
    /// Steps are executed in registration order - the first registered step is outermost
    /// and runs first. The terminal step forwards the serialized payload to the configured
    /// <see cref="IInterBusTransport"/> via <see cref="IInterBusTransportFactory"/>.
    /// </summary>
    /// <remarks>
    /// The schedule pipeline runs on already-serialized messages, unlike the publish pipeline.
    /// This makes it suitable for cross-cutting concerns that operate on raw bytes, such as
    /// the Claim Check pattern for large scheduled payloads.
    /// </remarks>
    IInterBusBuilder UseScheduleMiddleware<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TMiddleware>()
        where TMiddleware : class, IScheduleMiddleware;
}
