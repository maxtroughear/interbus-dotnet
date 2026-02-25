using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace InterBus.Internal;

/// <summary>
/// Scoped <see cref="IMessagePublisher"/> implementation. Routes messages through the registered
/// <see cref="IInterBusTransport"/> using convention-based topic and queue names.
/// Always stamps the <see cref="InterBusHeaders.MessageType"/> header so consumer dispatch
/// delegates can identify the message type without runtime reflection.
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
        var topicName = _options.TryGetTopicName(typeof(T), out var customName)
            ? customName
            : RoutingConventions.TopicName(typeof(T));
        var enriched = Enrich<T>(headers);
        var typeInfo = (JsonTypeInfo<T>)_options.SerializerOptions.GetTypeInfo(typeof(T));
        var payload = JsonSerializer.Serialize(message, typeInfo);
        return _transport.PublishToTopicAsync(topicName, payload, enriched, cancellationToken);
    }

    public Task Send<TConsumer, T>(T message, CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T>
        where T : class =>
        Send<TConsumer, T>(message, [], cancellationToken);

    public Task Send<TConsumer, T>(T message, MessageHeaders headers, CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T>
        where T : class
    {
        var queueName = RoutingConventions.QueueName(typeof(TConsumer));
        var enriched = Enrich<T>(headers);
        var typeInfo = (JsonTypeInfo<T>)_options.SerializerOptions.GetTypeInfo(typeof(T));
        var payload = JsonSerializer.Serialize(message, typeInfo);
        return _transport.SendToQueueAsync(queueName, payload, enriched, cancellationToken);
    }

    public Task Send<T>(string queueName, T message, CancellationToken cancellationToken = default)
        where T : class =>
        Send(queueName, message, [], cancellationToken);

    public Task Send<T>(string queueName, T message, MessageHeaders headers, CancellationToken cancellationToken = default)
        where T : class
    {
        var enriched = Enrich<T>(headers);
        var typeInfo = (JsonTypeInfo<T>)_options.SerializerOptions.GetTypeInfo(typeof(T));
        var payload = JsonSerializer.Serialize(message, typeInfo);
        return _transport.SendToQueueAsync(queueName, payload, enriched, cancellationToken);
    }

    /// <summary>
    /// Returns a copy of <paramref name="headers"/> with <see cref="InterBusHeaders.MessageType"/>
    /// stamped to the fully-qualified CLR type name of <typeparamref name="T"/>.
    /// </summary>
    private static MessageHeaders Enrich<T>(MessageHeaders headers) where T : class
    {
        var enriched = new MessageHeaders(headers)
        {
            [InterBusHeaders.MessageType] = typeof(T).FullName ?? typeof(T).Name
        };
        return enriched;
    }
}
