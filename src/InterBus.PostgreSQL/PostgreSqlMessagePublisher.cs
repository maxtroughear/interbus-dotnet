using Npgsql;

namespace InterBus.PostgreSQL;

/// <summary>
/// Scoped implementation of <see cref="IPostgreSqlMessagePublisher"/>.
/// <para>
/// All <see cref="IMessagePublisher"/> methods are forwarded to the standard inner publisher.
/// The additional transaction overloads serialize the message and delegate to
/// <see cref="PostgreSqlTransport.PublishInTransactionAsync"/> or
/// <see cref="PostgreSqlTransport.EnqueueInTransactionAsync"/>, enlisting the INSERT and
/// NOTIFY operations in the caller-owned transaction.
/// </para>
/// </summary>
public sealed class PostgreSqlMessagePublisher : IPostgreSqlMessagePublisher
{
    private readonly IMessagePublisher _inner;
    private readonly PostgreSqlTransport _transport;
    private readonly IMessageSerializer _serializer;
    private readonly InterBusOptions _options;

    public PostgreSqlMessagePublisher(
        IMessagePublisher inner,
        PostgreSqlTransport transport,
        IMessageSerializer serializer,
        InterBusOptions options)
    {
        _inner = inner;
        _transport = transport;
        _serializer = serializer;
        _options = options;
    }

    // ---- IMessagePublisher delegation ----

    public Task Publish<T>(T message, CancellationToken cancellationToken = default)
        where T : class => _inner.Publish(message, cancellationToken);

    public Task Publish<T>(T message, MessageHeaders headers, CancellationToken cancellationToken = default)
        where T : class => _inner.Publish(message, headers, cancellationToken);

    public Task Send<TConsumer, T>(T message, CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T> where T : class =>
        _inner.Send<TConsumer, T>(message, cancellationToken);

    public Task Send<TConsumer, T>(T message, MessageHeaders headers, CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T> where T : class =>
        _inner.Send<TConsumer, T>(message, headers, cancellationToken);

    public Task Send<T>(string queueName, T message, CancellationToken cancellationToken = default)
        where T : class => _inner.Send(queueName, message, cancellationToken);

    public Task Send<T>(string queueName, T message, MessageHeaders headers, CancellationToken cancellationToken = default)
        where T : class => _inner.Send(queueName, message, headers, cancellationToken);

    // ---- Transaction overloads ----

    public Task Publish<T>(T message, NpgsqlTransaction transaction,
        CancellationToken cancellationToken = default) where T : class =>
        Publish(message, [], transaction, cancellationToken);

    public async Task Publish<T>(T message, MessageHeaders headers, NpgsqlTransaction transaction,
        CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(transaction);
        var topicName = _options.TryGetTopicName(typeof(T), out var custom)
            ? custom
            : RoutingConventions.TopicName(typeof(T));
        var enriched = Enrich<T>(headers);
        await _transport.PublishInTransactionAsync(topicName, _serializer.Serialize(message),
            enriched, transaction, cancellationToken);
    }

    public Task Send<TConsumer, T>(T message, NpgsqlTransaction transaction,
        CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T> where T : class =>
        Send<TConsumer, T>(message, [], transaction, cancellationToken);

    public async Task Send<TConsumer, T>(T message, MessageHeaders headers, NpgsqlTransaction transaction,
        CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T> where T : class
    {
        ArgumentNullException.ThrowIfNull(transaction);
        var queueName = RoutingConventions.QueueName(typeof(TConsumer));
        var enriched = Enrich<T>(headers);
        await _transport.EnqueueInTransactionAsync(queueName, _serializer.Serialize(message),
            enriched, transaction, cancellationToken);
    }

    public Task Send<T>(string queueName, T message, NpgsqlTransaction transaction,
        CancellationToken cancellationToken = default) where T : class =>
        Send(queueName, message, [], transaction, cancellationToken);

    public async Task Send<T>(string queueName, T message, MessageHeaders headers,
        NpgsqlTransaction transaction, CancellationToken cancellationToken = default)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(transaction);
        var enriched = Enrich<T>(headers);
        await _transport.EnqueueInTransactionAsync(queueName, _serializer.Serialize(message),
            enriched, transaction, cancellationToken);
    }

    private static MessageHeaders Enrich<T>(MessageHeaders headers) where T : class
    {
        var enriched = new MessageHeaders(headers)
        {
            [InterBusHeaders.MessageType] = typeof(T).FullName ?? typeof(T).Name
        };
        return enriched;
    }
}
