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
    /// By default the InterBus schema (tables and indexes) is created automatically at application
    /// startup if it does not already exist. Set
    /// <see cref="PostgreSqlTransportOptions.RunSchemaMigrations"/> to <c>false</c> to manage
    /// the schema yourself via an external migration tool.
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
        builder.Services.AddSingleton<PostgreSqlTransport>();
        builder.Services.AddSingleton<IInterBusTransport>(sp => sp.GetRequiredService<PostgreSqlTransport>());

        RegisterCommonServices(builder, options);

        return builder;
    }

    /// <summary>
    /// Use a PostgreSQL-backed transport with an existing <see cref="NpgsqlDataSource"/>.
    /// Useful when the application already manages an <c>NpgsqlDataSource</c> in the DI container.
    /// <para>
    /// By default the InterBus schema (tables and indexes) is created automatically at application
    /// startup if it does not already exist. Set
    /// <see cref="PostgreSqlTransportOptions.RunSchemaMigrations"/> to <c>false</c> to manage
    /// the schema yourself via an external migration tool.
    /// </para>
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
        builder.Services.AddSingleton<PostgreSqlTransport>();
        builder.Services.AddSingleton<IInterBusTransport>(sp => sp.GetRequiredService<PostgreSqlTransport>());

        RegisterCommonServices(builder, options);

        return builder;
    }

    /// <summary>
    /// Registers services that are common to both overloads: the optional schema initializer
    /// (controlled by <see cref="PostgreSqlTransportOptions.RunSchemaMigrations"/>) and the
    /// message pump. Hosted services start in registration order, so the schema initializer is
    /// always registered before the pump.
    /// </summary>
    private static void RegisterCommonServices(IInterBusBuilder builder, PostgreSqlTransportOptions options)
    {
        // Always registered so consumers can optionally inject it to share the pump's transaction.
        builder.Services.AddScoped<PostgreSqlConsumeContext>();

        // Register the PostgreSQL-specific publisher that exposes transaction overloads.
        // Users who need atomic publish-with-business-logic inject IPostgreSqlMessagePublisher;
        // users who don't can continue injecting IMessagePublisher.
        builder.Services.AddScoped<IPostgreSqlMessagePublisher, PostgreSqlMessagePublisher>();

        if (options.RunSchemaMigrations)
        {
            builder.Services.AddSingleton<PostgreSqlSchemaManager>();
            // Schema initializer must start before the message pump so the tables exist before
            // the pump tries to SELECT from them.
            builder.Services.AddHostedService<PostgreSqlSchemaInitializer>();
        }

        builder.Services.AddHostedService<PostgreSqlMessagePump>();
    }
}
