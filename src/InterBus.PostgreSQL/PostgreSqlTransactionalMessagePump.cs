using InterBus.Consuming;
using InterBus.MessagePumps;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace InterBus.PostgreSQL;

internal sealed partial class PostgreSqlTransactionalMessagePump(
    ConsumerDescriptor descriptor, NpgsqlDataSource dataSource, PostgreSqlTransportOptions options,
    InterBusOptions busOptions, ILogger<PostgreSqlTransactionalMessagePump> logger)
    : PostgreSqlConsumerSource(descriptor, dataSource, options, busOptions, logger), IManagedSource
{
    public async Task RunAsync(Func<IDelivery, CancellationToken, Task> process, CancellationToken intakeToken,
        CancellationToken processingToken)
    {
        using var workers = CancellationTokenSource.CreateLinkedTokenSource(intakeToken);
        await Task.WhenAll(Enumerable.Range(0, Descriptor.ConcurrencyCount).Select(_ => WorkerAsync()));

        async Task WorkerAsync()
        {
            try
            {
                await PostgreSqlConnectionRetry.ExecuteAsync(async ct =>
                {
                    while (!ct.IsCancellationRequested)
                    {
                        var version = WorkVersion;
                        var processed = await ProcessNextAsync(process, ct, processingToken);
                        if (!processed) await WaitForWorkAsync(version, ct);
                    }
                }, Options, logger, workers.Token);
            }
            catch (OperationCanceledException) when (workers.IsCancellationRequested) { }
            catch (Exception error) { ReportFailure(error); await workers.CancelAsync(); throw; }
        }
    }

    private async Task<bool> ProcessNextAsync(Func<IDelivery, CancellationToken, Task> process,
        CancellationToken intakeToken, CancellationToken processingToken)
    {
        await using var connection = await DataSource.OpenConnectionAsync(intakeToken);
        await using var transaction = await connection.BeginTransactionAsync(intakeToken);
        await using var select = connection.CreateCommand();
        select.Transaction = transaction;
        select.CommandText = $"SELECT * FROM \"{EscapeIdentifier(Options.SchemaName)}\".consume_next($1)";
        select.Parameters.Add(new NpgsqlParameter<long> { TypedValue = QueueId });
        await using var reader = await select.ExecuteReaderAsync(intakeToken);
        if (!await reader.ReadAsync(intakeToken)) return false;
        var id = reader.GetInt64(0);
        var payload = (byte[])reader.GetValue(1);
        var headers = reader.GetString(2);
        var payloadId = reader.GetInt64(3);
        await reader.CloseAsync();

        // Establish ambient state in the same execution flow that awaits core processing
        var previous = PostgreSqlTransactionContext.Current.Value;
        PostgreSqlTransactionContext.Current.Value = transaction;
        try
        {
            var delivery = new TransactionDelivery(connection, transaction, Options.SchemaName,
                Descriptor.QueueName, id, payloadId, payload, headers);
            await process(delivery, processingToken);
            return delivery.Acknowledged;
        }
        finally { PostgreSqlTransactionContext.Current.Value = previous; }
    }
}
