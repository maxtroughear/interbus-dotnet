using Amazon.DynamoDBv2.Model;

namespace InterBus.DynamoDB.Outbox;

/// <summary>
/// Scoped context for the InterBus DynamoDB transactional outbox.
/// <para>
/// Add your application's DynamoDB write operations via <see cref="AddTransactItem"/>, then call
/// <see cref="CommitAsync"/> to atomically commit them together with any messages published via
/// <see cref="IPublisher"/> in the same DI scope. The commit uses a single DynamoDB
/// <c>TransactWriteItems</c> call, providing all-or-nothing atomicity across your business
/// data and the outbox messages.
/// </para>
/// <para>
/// A background processor picks up the committed outbox messages and forwards them to the
/// configured transport.
/// </para>
/// </summary>
/// <example>
/// <code>
/// await using var scope = serviceProvider.CreateAsyncScope();
/// var publisher = scope.ServiceProvider.GetRequiredService&lt;IPublisher&gt;();
/// var outbox = scope.ServiceProvider.GetRequiredService&lt;IDynamoDbOutboxContext&gt;();
///
/// outbox.AddTransactItem(new TransactWriteItem { Put = new Put { ... } });
/// await publisher.PublishAsync(new OrderPlaced(...));
///
/// await outbox.CommitAsync();
/// </code>
/// </example>
public interface IDynamoDbOutboxContext
{
    /// <summary>
    /// Adds a DynamoDB <see cref="TransactWriteItem"/> to the current transaction batch.
    /// These items are written atomically with any pending outbox messages on the next
    /// <see cref="CommitAsync"/> call.
    /// </summary>
    void AddTransactItem(TransactWriteItem item);

    /// <summary>
    /// Atomically writes all registered <see cref="TransactWriteItem"/>s together with any
    /// pending outbox messages using a single DynamoDB <c>TransactWriteItems</c> request.
    /// <para>
    /// If no items are pending this method returns immediately without making any DynamoDB
    /// calls.
    /// </para>
    /// </summary>
    Task CommitAsync(CancellationToken cancellationToken = default);
}
