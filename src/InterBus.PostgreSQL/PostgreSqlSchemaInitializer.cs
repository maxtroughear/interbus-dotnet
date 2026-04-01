using Microsoft.Extensions.Hosting;

namespace InterBus.PostgreSQL;

/// <summary>
/// Applies the InterBus PostgreSQL schema migration during application startup.
/// Registered as the first hosted service so that the messages table exists before
/// the <see cref="PostgreSqlMessagePump"/> begins listening.
/// </summary>
internal sealed class PostgreSqlSchemaInitializer : IHostedService
{
    private readonly PostgreSqlSchemaManager _schemaManager;

    public PostgreSqlSchemaInitializer(PostgreSqlSchemaManager schemaManager)
    {
        _schemaManager = schemaManager;
    }

    /// <inheritdoc/>
    public Task StartAsync(CancellationToken cancellationToken) =>
        _schemaManager.MigrateAsync(cancellationToken);

    /// <inheritdoc/>
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
