using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;

namespace InterBus.PostgreSQL.Extensions;

/// <summary>Extension methods for configuring the PostgreSQL transport on an <see cref="IInterBusBuilder"/>.</summary>
public static class PostgreSqlTransportBuilderExtensions
{
    /// <summary>
    /// Use a PostgreSQL-backed transport with <c>LISTEN</c>/<c>NOTIFY</c> for efficient message consumption.
    /// <para>
    /// The InterBus schema (table and indexes) is created automatically at application startup
    /// if it does not already exist.
    /// </para>
    /// </summary>
    /// <param name="builder">The InterBus builder.</param>
    /// <param name="configure">
    /// Delegate to configure <see cref="PostgreSqlTransportOptions"/>. At minimum set
    /// <see cref="PostgreSqlTransportOptions.ConnectionString"/>.
    /// </param>
    public static IInterBusBuilder AddPostgreSqlTransport(
        this IInterBusBuilder builder,
        Action<PostgreSqlTransportOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new PostgreSqlTransportOptions();
        configure(options);

        if (string.IsNullOrWhiteSpace(options.ConnectionString))
            throw new InvalidOperationException(
                "PostgreSqlTransportOptions.ConnectionString must be set before calling AddPostgreSqlTransport.");

        builder.Services.AddSingleton(options);
        builder.Services.AddSingleton(NpgsqlDataSource.Create(options.ConnectionString));
        builder.Services.AddSingleton<PostgreSqlSchemaManager>();
        builder.Services.AddSingleton<PostgreSqlTransport>();
        builder.Services.AddSingleton<IInterBusTransport>(sp => sp.GetRequiredService<PostgreSqlTransport>());

        // Schema initializer must start before the message pump so the table exists before
        // the pump tries to SELECT from it. Hosted services start in registration order.
        builder.Services.AddHostedService<PostgreSqlSchemaInitializer>();
        builder.Services.AddHostedService<PostgreSqlMessagePump>();

        return builder;
    }

    /// <summary>
    /// Use a PostgreSQL-backed transport with an existing <see cref="NpgsqlDataSource"/>.
    /// Useful when the application already manages an <c>NpgsqlDataSource</c> in the DI container.
    /// </summary>
    /// <param name="builder">The InterBus builder.</param>
    /// <param name="dataSource">The Npgsql data source to use for all database operations.</param>
    /// <param name="configure">
    /// Optional delegate to configure additional <see cref="PostgreSqlTransportOptions"/> (e.g. schema name).
    /// The <see cref="PostgreSqlTransportOptions.ConnectionString"/> is not used when a data source
    /// is provided directly.
    /// </param>
    public static IInterBusBuilder AddPostgreSqlTransport(
        this IInterBusBuilder builder,
        NpgsqlDataSource dataSource,
        Action<PostgreSqlTransportOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(dataSource);

        var options = new PostgreSqlTransportOptions();
        configure?.Invoke(options);

        builder.Services.AddSingleton(options);
        builder.Services.AddSingleton(dataSource);
        builder.Services.AddSingleton<PostgreSqlSchemaManager>();
        builder.Services.AddSingleton<PostgreSqlTransport>();
        builder.Services.AddSingleton<IInterBusTransport>(sp => sp.GetRequiredService<PostgreSqlTransport>());

        builder.Services.AddHostedService<PostgreSqlSchemaInitializer>();
        builder.Services.AddHostedService<PostgreSqlMessagePump>();

        return builder;
    }
}
