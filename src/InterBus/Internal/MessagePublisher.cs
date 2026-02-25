using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace InterBus.Internal;

/// <summary>
/// Scoped <see cref="IMessagePublisher"/> implementation. Routes messages through the registered
/// <see cref="IInterBusTransport"/> using convention-based topic and queue names.
/// </summary>
internal sealed class MessagePublisher : IMessagePublisher
{
    private readonly IInterBusTransport _transport;
    private readonly InterBusOptions _options;

    public MessagePublisher(IInterBusTransport transport, InterBusOptions options)
    {
        _transport = transport;
        _options = options;
    }

    public Task Publish<T>(T message, CancellationToken cancellationToken = default) where T : class =>
        Publish(message, [], cancellationToken);

    public Task Publish<T>(T message, MessageHeaders headers, CancellationToken cancellationToken = default)
        where T : class
    {
        var topicName = RoutingConventions.TopicName(typeof(T));
        var typeInfo = (JsonTypeInfo<T>)_options.SerializerOptions.GetTypeInfo(typeof(T));
        var payload = JsonSerializer.SerializeToUtf8Bytes(message, typeInfo);
        return _transport.PublishToTopicAsync(topicName, payload, headers, cancellationToken);
    }

    public Task Send<TConsumer, T>(T message, CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T>
        where T : class =>
        Send<TConsumer, T>(message, [], cancellationToken);

    public Task Send<TConsumer, T>(T message, MessageHeaders headers, CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T>
        where T : class
    {
        var queueName = RoutingConventions.QueueName(typeof(TConsumer), typeof(T));
        var typeInfo = (JsonTypeInfo<T>)_options.SerializerOptions.GetTypeInfo(typeof(T));
        var payload = JsonSerializer.SerializeToUtf8Bytes(message, typeInfo);
        return _transport.SendToQueueAsync(queueName, payload, headers, cancellationToken);
    }
}
