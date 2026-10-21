using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace InterBus.PostgreSQL;

internal static class PostgreSqlConnectionRetry
{
    private static bool IsConnectionRelated(Exception exception)
    {
        if (exception is PostgresException postgres)
            return postgres.SqlState.StartsWith("08", StringComparison.Ordinal) ||
                postgres.SqlState is PostgresErrorCodes.AdminShutdown or PostgresErrorCodes.CrashShutdown
                    or PostgresErrorCodes.CannotConnectNow;

        return exception is NpgsqlException
               || FindInner<SocketException>(exception) is not null
               || FindInner<TimeoutException>(exception) is not null;
    }

    public static async Task<T> ExecuteAsync<T>(
        Func<CancellationToken, Task<T>> operation,
        PostgreSqlTransportOptions options,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        var attempt = 0;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                return await operation(cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex) when (ShouldRetry(ex, options, attempt + 1))
            {
                var delay = GetDelay(options, ++attempt);
                logger.LogWarning(
                    ex,
                    "InterBus PostgreSQL: failed due to a PostgreSQL connection error; retrying in {Delay}",
                    delay);
                await Task.Delay(delay, cancellationToken);
            }
        }
    }

    public static Task ExecuteAsync(
        Func<CancellationToken, Task> operation,
        PostgreSqlTransportOptions options,
        ILogger logger,
        CancellationToken cancellationToken) =>
        ExecuteAsync(
            async ct =>
            {
                await operation(ct);
                return true;
            },
            options,
            logger,
            cancellationToken);

    private static bool ShouldRetry(Exception exception, PostgreSqlTransportOptions options, int attempt)
    {
        if (!IsConnectionRelated(exception))
            return false;

        var retry = options.ConnectionRetry;
        if (attempt > retry.RetryCount)
            return false;

        return retry.ShouldRetry?.Invoke(exception) ?? true;
    }

    private static TimeSpan GetDelay(PostgreSqlTransportOptions options, int attempt)
    {
        var delay = options.ConnectionRetry.DelayProvider(attempt);
        return delay > TimeSpan.Zero ? delay : TimeSpan.Zero;
    }

    private static TException? FindInner<TException>(Exception exception)
        where TException : Exception
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is TException match)
                return match;
        }

        return null;
    }
}
