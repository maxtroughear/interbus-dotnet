using System.Threading.Channels;
using InterBus.Consuming;
using InterBus.MessagePumps;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace InterBus.PostgreSQL;

internal abstract class PostgreSqlConsumerSource(
    ConsumerDescriptor descriptor, NpgsqlDataSource dataSource, PostgreSqlTransportOptions options,
    InterBusOptions busOptions, ILogger logger) : IConsumerSource
{
    protected readonly ConsumerDescriptor Descriptor = descriptor;
    protected readonly NpgsqlDataSource DataSource = dataSource;
    protected readonly PostgreSqlTransportOptions Options = options;
    protected long QueueId;
    private readonly TaskCompletionSource<Exception> _failure = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public Task<Exception> Failure => _failure.Task;
    protected void ReportFailure(Exception error) => _failure.TrySetResult(error);
    private readonly CancellationTokenSource _listenerStop = new();
    private readonly Channel<bool> _notifications = Channel.CreateBounded<bool>(new BoundedChannelOptions(1)
    { FullMode = BoundedChannelFullMode.DropWrite, SingleWriter = true, SingleReader = false });
    private readonly object _gate = new();
    private Task? _listener;
    protected static string EscapeIdentifier(string name) => name.Replace("\"", "\"\"");

    public Task InitializeAsync(CancellationToken cancellationToken) => PostgreSqlConnectionRetry.ExecuteAsync(async ct =>
    {
        if (this is IManagedSource && Descriptor.PumpCount != 1)
            throw new InvalidOperationException("PostgreSQL transactional consumers support PumpCount = 1 only");
        await using var connection = await DataSource.OpenConnectionAsync(ct);
        await using var queue = connection.CreateCommand();
        queue.CommandText = $"SELECT \"{EscapeIdentifier(Options.SchemaName)}\".ensure_queue($1)";
        queue.Parameters.Add(new NpgsqlParameter<string> { TypedValue = Descriptor.QueueName });
        QueueId = (long)(await queue.ExecuteScalarAsync(ct))!;
        foreach (var messageType in Descriptor.MessageTypes)
        {
            await using var subscription = connection.CreateCommand();
            subscription.CommandText = $"SELECT \"{EscapeIdentifier(Options.SchemaName)}\".ensure_subscription($1, $2)";
            subscription.Parameters.Add(new NpgsqlParameter<long> { TypedValue = QueueId });
            subscription.Parameters.Add(new NpgsqlParameter<string> { TypedValue = busOptions.GetPublisherDescriptor(messageType).TopicName });
            await subscription.ExecuteNonQueryAsync(ct);
        }
    }, Options, logger, cancellationToken);

    protected async Task WaitForWorkAsync(CancellationToken cancellationToken)
    {
        Task listener;
        lock (_gate) listener = _listener ??= ListenAsync();
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(Options.FallbackPollInterval);
        var notification = _notifications.Reader.ReadAsync(timeout.Token).AsTask();
        if (await Task.WhenAny(listener, notification) == listener)
        {
            await timeout.CancelAsync();
            try { await notification; } catch (OperationCanceledException) { }
            await listener;
        }
        else
        {
            try { await notification; }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested) { }
        }
    }

    private async Task ListenAsync()
    {
        try { await ListenWithRetryAsync(); }
        catch (OperationCanceledException) when (_listenerStop.IsCancellationRequested) { }
        catch (Exception error) { ReportFailure(error); throw; }
    }

    private Task ListenWithRetryAsync() => PostgreSqlConnectionRetry.ExecuteAsync(async ct =>
    {
        await using var connection = await DataSource.OpenConnectionAsync(ct);
        connection.Notification += (_, _) => _notifications.Writer.TryWrite(true);
        await using var command = connection.CreateCommand();
        command.CommandText = $"LISTEN \"ib_{QueueId}\"";
        await command.ExecuteNonQueryAsync(ct);
        while (!ct.IsCancellationRequested)
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeout.CancelAfter(Options.FallbackPollInterval);
            try { await connection.WaitAsync(timeout.Token); }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                await using var cleanup = DataSource.CreateCommand();
                cleanup.CommandText = $"SELECT \"{EscapeIdentifier(Options.SchemaName)}\".cleanup_orphaned_payloads()";
                await cleanup.ExecuteNonQueryAsync(ct);
            }
        }
    }, Options, logger, _listenerStop.Token);

    public async ValueTask DisposeAsync()
    {
        await _listenerStop.CancelAsync();
        try { if (_listener is not null) await _listener; }
        catch (OperationCanceledException) when (_listenerStop.IsCancellationRequested) { }
        finally { _listenerStop.Dispose(); }
    }
}

internal sealed class PostgreSqlConsumerSourceFactory(IServiceProvider provider, PostgreSqlTransportOptions options)
    : IConsumerSourceFactory
{
    public IConsumerSource Create(ConsumerDescriptor descriptor) =>
        options.MessagePumpMode == PostgreSqlMessagePumpMode.Lease
            ? ActivatorUtilities.CreateInstance<PostgreSqlLeaseMessagePump>(provider, descriptor)
            : ActivatorUtilities.CreateInstance<PostgreSqlTransactionalMessagePump>(provider, descriptor);
}
