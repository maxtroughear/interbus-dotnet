using System.Diagnostics;
using Npgsql;

namespace InterBus.PostgreSQL.Tests;

internal sealed class PostgreSqlLoadProgress : IAsyncDisposable
{
    private readonly NpgsqlDataSource _observer;
    private readonly Func<int> _received;
    private readonly CancellationTokenSource _stop = new();
    private readonly Stopwatch _elapsed = Stopwatch.StartNew();
    private readonly Task _monitor;
    private int _published;

    public PostgreSqlLoadProgress(string connectionString, Func<int> received)
    {
        var settings = new NpgsqlConnectionStringBuilder(connectionString)
        { ApplicationName = "load-observer", MaxPoolSize = 1, CommandTimeout = 3, Timeout = 3 };
        // Use a separate pool so diagnostics still work when the transport's pool is exhausted
        _observer = NpgsqlDataSource.Create(settings.ConnectionString);
        _received = received;
        _monitor = MonitorAsync();
    }

    public void Published() => Interlocked.Increment(ref _published);

    private async Task MonitorAsync()
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(15));
        try
        {
            while (await timer.WaitForNextTickAsync(_stop.Token))
            {
                await TestContext.Progress.WriteLineAsync(
                    $"Load progress {_elapsed.Elapsed.TotalSeconds:F0}s: published={Volatile.Read(ref _published)}, received={_received()}");
                try
                {
                    await using var command = _observer.CreateCommand("""
                        SELECT state, COALESCE(wait_event_type || ':' || wait_event, 'running'), COUNT(*),
                               COALESCE(MAX(EXTRACT(EPOCH FROM clock_timestamp() - xact_start)), 0)::double precision,
                               LEFT(query, 160)
                        FROM pg_stat_activity
                        WHERE datname = current_database() AND pid <> pg_backend_pid()
                        GROUP BY state, wait_event_type, wait_event, LEFT(query, 160)
                        ORDER BY COUNT(*) DESC LIMIT 8
                        """);
                    await using var reader = await command.ExecuteReaderAsync(_stop.Token);
                    while (await reader.ReadAsync(_stop.Token))
                        await TestContext.Progress.WriteLineAsync(
                            $"DB {reader.GetString(0)} {reader.GetString(1)} count={reader.GetInt64(2)} oldest_tx={reader.GetDouble(3):F1}s {reader.GetString(4)}");
                }
                catch (NpgsqlException error)
                { await TestContext.Progress.WriteLineAsync($"Load diagnostics unavailable: {error.Message}"); }
            }
        }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
    }

    public async ValueTask DisposeAsync()
    {
        await _stop.CancelAsync();
        try { await _monitor; }
        finally { _stop.Dispose(); await _observer.DisposeAsync(); }
    }
}
