using System.Text.Json;

namespace InterBus.Internal;

/// <summary>
/// Scoped <see cref="IMessagePublisher"/> implementation. Routes messages through the registered
/// <see cref="IInterBusTransport"/> using convention-based topic and queue names.
/// </summary>
internal sealed class MessagePublisher : IMessagePublisher
{
    private readonly IInterBusTransport _transport;

    public MessagePublisher(IInterBusTransport transport)
    {
        _transport = transport;
    }

    public Task Publish<T>(T message, CancellationToken cancellationToken = default) where T : class
    {
        var topicName = RoutingConventions.TopicName(typeof(T));
        var payload = JsonSerializer.SerializeToUtf8Bytes(message);
        return _transport.PublishToTopicAsync(topicName, payload, cancellationToken);
    }

    public Task Send<TConsumer, T>(T message, CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T>
        where T : class
    {
        var queueName = RoutingConventions.QueueName(typeof(TConsumer), typeof(T));
        var payload = JsonSerializer.SerializeToUtf8Bytes(message);
        return _transport.SendToQueueAsync(queueName, payload, cancellationToken);
    }
}
