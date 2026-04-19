using InterBus.Consumer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace InterBus.PostgreSQL;

/// <summary>
/// Transaction-based PostgreSQL message pump.
/// <para>
/// Holds ownership of each in-flight message by keeping a database transaction open around
/// <c>consume_next</c> and message dispatch. This is retained for specialised scenarios where
/// transaction-held row locks are preferred over renewable visibility leases.
/// </para>
/// </summary>
internal class PostgreSqlTransactionalMessagePump : PostgreSqlMessagePumpBase
{
    private readonly ILogger<PostgreSqlTransactionalMessagePump> _logger;

    /// <summary>
    /// Transaction-based PostgreSQL message pump
    /// <para>
    /// Holds ownership of each in-flight message by keeping a database transaction open around
    /// <c>consume_next</c> and message dispatch. This is retained for specialised scenarios where
    /// transaction-held row locks are preferred over renewable visibility leases.
    /// </para>
    /// </summary>
    public PostgreSqlTransactionalMessagePump(NpgsqlDataSource dataSource,
        PostgreSqlTransportOptions options,
        InterBusOptions busOptions,
        IServiceScopeFactory scopeFactory,
        ILogger<PostgreSqlTransactionalMessagePump> logger) : base(dataSource, options, busOptions, scopeFactory,
        logger)
    {
        _logger = logger;
    }

    protected override string PumpModeName => "transactional";

    /// <summary>
    /// Attempts to fetch and dispatch the next pending message for <paramref name="descriptor"/>.
    /// Uses <c>consume_next</c> (SELECT FOR UPDATE SKIP LOCKED) inside an explicit transaction
    /// so the row lock persists until the consumer succeeds or the transaction is rolled back.
    /// On success, <c>ack_message</c> deletes the routing row and conditionally the payload in
    /// a single round trip.
    /// </summary>
    /// <returns><c>true</c> if a message was dispatched; <c>false</c> if the queue was empty.</returns>
    protected override async Task<bool> TryProcessNextAsync(ConsumerDescriptor descriptor, long queueId,
        CancellationToken stoppingToken)
    {
        await using var conn = await DataSource.OpenConnectionAsync(stoppingToken);
        await using var tx = await conn.BeginTransactionAsync(stoppingToken);

        // Lock one pending message routing row without locking the shared payload row so
        // concurrent pumps on different queues don't block each other.
        await using var selectCmd = conn.CreateCommand();
        selectCmd.Transaction = tx;
        selectCmd.CommandText =
            $"SELECT * FROM \"{EscapeIdentifier(Options.SchemaName)}\".consume_next($1)";
        selectCmd.Parameters.Add(new NpgsqlParameter<long> { TypedValue = queueId });

        await using var reader = await selectCmd.ExecuteReaderAsync(stoppingToken);
        if (!await reader.ReadAsync(stoppingToken))
        {
            await reader.CloseAsync();
            await tx.RollbackAsync(CancellationToken.None);
            return false;
        }

        var id = reader.GetInt64(0);
        var payload = (byte[])reader.GetValue(1);
        var headersJson = reader.GetString(2);
        var payloadId = reader.GetInt64(3);
        await reader.CloseAsync();

        var headers = PostgreSqlTransport.DeserializeHeaders(headersJson);

        // Make the transaction available to consumers via the extension method on IConsumeContext.
        // AsyncLocal flows the value into all child async operations (including the consumer's
        // Consume method) without affecting the caller's execution context.
        PostgreSqlTransactionContext.Current.Value = tx;
        try
        {
            await DispatchAsync(descriptor, payload, headers, stoppingToken);

            // Delete the routing row and (when unreferenced) the shared payload in one round trip.
            await using var ackCmd = conn.CreateCommand();
            ackCmd.Transaction = tx;
            ackCmd.CommandText =
                $"SELECT \"{EscapeIdentifier(Options.SchemaName)}\".ack_message($1, $2)";
            ackCmd.Parameters.Add(new NpgsqlParameter<long> { TypedValue = id });
            ackCmd.Parameters.Add(new NpgsqlParameter<long> { TypedValue = payloadId });
            await ackCmd.ExecuteNonQueryAsync(stoppingToken);

            await tx.CommitAsync(stoppingToken);
            return true;
        }
        catch (OperationCanceledException)
        {
            await tx.RollbackAsync(CancellationToken.None);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "InterBus PostgreSQL: transactional consumer {ConsumerType} failed for queue {QueueName}; message will be retried",
                descriptor.ConsumerType.Name,
                descriptor.QueueName);

            // Roll back so the message remains in the table for the next delivery attempt.
            await tx.RollbackAsync(CancellationToken.None);

            // Return false so the processing loop backs off; the message will be re-delivered
            // on the next NOTIFY or on the next fallback poll.
            return false;
        }
        finally
        {
            PostgreSqlTransactionContext.Current.Value = null;
        }
    }
}
