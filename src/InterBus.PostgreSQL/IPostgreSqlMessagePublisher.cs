using Npgsql;

namespace InterBus.PostgreSQL;

/// <summary>
/// Extends <see cref="IMessagePublisher"/> with overloads that accept a caller-owned
/// <see cref="NpgsqlTransaction"/>, enabling atomic publish-with-business-logic without an
/// outbox pattern.
/// <para>
/// Inject this interface instead of <see cref="IMessagePublisher"/> when you need to publish
/// or send messages within an existing database transaction. All other methods are identical to
/// <see cref="IMessagePublisher"/>.
/// </para>
/// <para>
/// The caller is responsible for committing or rolling back the transaction; InterBus only
/// enlists the INSERT and NOTIFY operations in the supplied transaction.
/// </para>
/// </summary>
public interface IPostgreSqlMessagePublisher : IMessagePublisher
{
    /// <summary>
    /// Publish a message to the topic for <typeparamref name="T"/>, enlisting the INSERT and
    /// NOTIFY in <paramref name="transaction"/>. All registered consumers for this message type
    /// will receive the message after the transaction commits.
    /// </summary>
    Task Publish<T>(T message, NpgsqlTransaction transaction,
        CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Publish a message to the topic for <typeparamref name="T"/> with additional
    /// <paramref name="headers"/>, enlisting the INSERT and NOTIFY in
    /// <paramref name="transaction"/>.
    /// </summary>
    Task Publish<T>(T message, MessageHeaders headers, NpgsqlTransaction transaction,
        CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Send a message directly to the queue of <typeparamref name="TConsumer"/>, enlisting
    /// the INSERT and NOTIFY in <paramref name="transaction"/>.
    /// </summary>
    Task Send<TConsumer, T>(T message, NpgsqlTransaction transaction,
        CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T>
        where T : class;

    /// <summary>
    /// Send a message directly to the queue of <typeparamref name="TConsumer"/> with additional
    /// <paramref name="headers"/>, enlisting the INSERT and NOTIFY in
    /// <paramref name="transaction"/>.
    /// </summary>
    Task Send<TConsumer, T>(T message, MessageHeaders headers, NpgsqlTransaction transaction,
        CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T>
        where T : class;

    /// <summary>
    /// Send a message directly to the named <paramref name="queueName"/>, enlisting the INSERT
    /// and NOTIFY in <paramref name="transaction"/>.
    /// </summary>
    Task Send<T>(string queueName, T message, NpgsqlTransaction transaction,
        CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Send a message directly to the named <paramref name="queueName"/> with additional
    /// <paramref name="headers"/>, enlisting the INSERT and NOTIFY in
    /// <paramref name="transaction"/>.
    /// </summary>
    Task Send<T>(string queueName, T message, MessageHeaders headers,
        NpgsqlTransaction transaction, CancellationToken cancellationToken = default)
        where T : class;
}
