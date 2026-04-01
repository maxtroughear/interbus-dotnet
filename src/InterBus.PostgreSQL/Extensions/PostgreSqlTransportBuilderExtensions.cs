using Microsoft.Extensions.DependencyInjection;
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
    /// <para>
    /// Connection-related PostgreSQL failures during schema startup and message-pump listener setup
    /// are retried automatically using the defaults on
    /// <see cref="PostgreSqlTransportOptions.ConnectionRetry"/>.
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

        builder.Options.SetTransportUriSchemes(PostgreSqlTransport.TopicScheme, PostgreSqlTransport.QueueScheme);
        builder.Services.AddSingleton(options);
        builder.Services.AddSingleton(NpgsqlDataSource.Create(options.ConnectionString));
        RegisterTransportServices(builder);

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
    /// <para>
    /// Connection-related PostgreSQL failures during schema startup and message-pump listener setup
    /// are retried automatically using the defaults on
    /// <see cref="PostgreSqlTransportOptions.ConnectionRetry"/>.
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

        builder.Options.SetTransportUriSchemes(PostgreSqlTransport.TopicScheme, PostgreSqlTransport.QueueScheme);
        builder.Services.AddSingleton(options);
        builder.Services.AddSingleton(dataSource);
        RegisterTransportServices(builder);

        RegisterCommonServices(builder, options);

        return builder;
    }

    /// <summary>
    /// Registers <see cref="PostgreSqlTransport"/> as its own concrete type, as a non-keyed
    /// <see cref="IInterBusTransport"/>, and as a keyed service for each supported URI scheme
    /// so the transport factory can resolve it by destination scheme.
    /// </summary>
    private static void RegisterTransportServices(IInterBusBuilder builder)
    {
        // Register the concrete type so keyed registrations can share the same singleton.
        builder.Services.AddSingleton<PostgreSqlTransport>();

        // Keyed registrations for transport factory scheme-based resolution.
        builder.Services.AddKeyedSingleton<IInterBusTransport>(PostgreSqlTransport.TopicScheme,
            (sp, _key) => sp.GetRequiredService<PostgreSqlTransport>());
        builder.Services.AddKeyedSingleton<IInterBusTransport>(PostgreSqlTransport.QueueScheme,
            (sp, _key) => sp.GetRequiredService<PostgreSqlTransport>());

        // Keep the non-keyed registration for backward compatibility.
        builder.Services.AddSingleton<IInterBusTransport>(sp => sp.GetRequiredService<PostgreSqlTransport>());
    }

    /// <summary>
    /// Registers services that are common to both overloads: the optional schema initializer
    /// (controlled by <see cref="PostgreSqlTransportOptions.RunSchemaMigrations"/>) and the
    /// message pump. Hosted services start in registration order, so the schema initializer is
    /// always registered before the pump.
    /// </summary>
    private static void RegisterCommonServices(IInterBusBuilder builder, PostgreSqlTransportOptions options)
    {
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
