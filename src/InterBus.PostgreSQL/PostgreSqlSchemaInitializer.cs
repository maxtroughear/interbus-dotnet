using Microsoft.Extensions.Hosting;

namespace InterBus.PostgreSQL;

/// <summary>
/// Applies the InterBus PostgreSQL schema migration during application startup.
/// Registered as the first hosted service so that the messages table exists before
/// the configured PostgreSQL message pump begins listening.
/// </summary>
internal sealed class PostgreSqlSchemaInitializer(PostgreSqlSchemaManager schemaManager) : IHostedService
{
    /// <inheritdoc/>
    public Task StartAsync(CancellationToken cancellationToken) =>
        schemaManager.MigrateAsync(cancellationToken);

    /// <inheritdoc/>
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
