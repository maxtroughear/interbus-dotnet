using InterBus.Transport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;

namespace InterBus.PostgreSQL.Extensions;

/// <summary>Extension methods for configuring the PostgreSQL transport on an <see cref="IInterBusBuilder"/>.</summary>
public static class PostgreSqlTransportBuilderExtensions
{
    /// <param name="builder">The InterBus builder.</param>
    extension(IInterBusBuilder builder)
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
        /// The default message-pump mode is <see cref="PostgreSqlMessagePumpMode.Lease"/>, which
        /// uses renewable visibility leases so long-running consumers do not hold a processing
        /// connection for the full duration of the handler.
        /// </para>
        /// </summary>
        /// <param name="configure">
        /// Delegate to configure <see cref="PostgreSqlTransportOptions"/>. At minimum set
        /// <see cref="PostgreSqlTransportOptions.ConnectionString"/>.
        /// </param>
        public IInterBusBuilder AddPostgreSqlTransport(Action<PostgreSqlTransportOptions> configure)
        {
            ArgumentNullException.ThrowIfNull(builder);
            ArgumentNullException.ThrowIfNull(configure);

            var options = new PostgreSqlTransportOptions();
            configure(options);

            if (string.IsNullOrWhiteSpace(options.ConnectionString))
                throw new InvalidOperationException(
                    "PostgreSqlTransportOptions.ConnectionString must be set before calling AddPostgreSqlTransport.");

            ValidateOptions(options);

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
        /// <param name="dataSource">The Npgsql data source to use for all database operations.</param>
        /// <param name="configure">
        /// Optional delegate to configure additional <see cref="PostgreSqlTransportOptions"/> (e.g. schema name).
        /// The <see cref="PostgreSqlTransportOptions.ConnectionString"/> is not used when a data source
        /// is provided directly.
        /// </param>
        public IInterBusBuilder AddPostgreSqlTransport(NpgsqlDataSource dataSource,
            Action<PostgreSqlTransportOptions>? configure = null)
        {
            ArgumentNullException.ThrowIfNull(builder);
            ArgumentNullException.ThrowIfNull(dataSource);

            var options = new PostgreSqlTransportOptions();
            configure?.Invoke(options);

            ValidateOptions(options);

            builder.Options.SetTransportUriSchemes(PostgreSqlTransport.TopicScheme, PostgreSqlTransport.QueueScheme);
            builder.Services.AddSingleton(options);
            builder.Services.AddSingleton(dataSource);
            RegisterTransportServices(builder);

            RegisterCommonServices(builder, options);

            return builder;
        }
    }

    /// <summary>
    /// Registers <see cref="PostgreSqlTransport"/> as its own concrete type, as a non-keyed
    /// <see cref="ITransport"/>, and as a keyed service for each supported URI scheme
    /// so the transport factory can resolve it by destination scheme.
    /// </summary>
    private static void RegisterTransportServices(IInterBusBuilder builder)
    {
        builder.Services.AddSingleton<ITopologyBuilder, PostgreSqlTopologyBuilder>();

        // Register the concrete type so keyed registrations can share the same singleton.
        builder.Services.AddSingleton<PostgreSqlTransport>();

        // Keyed registrations for transport factory scheme-based resolution.
        builder.Services.AddKeyedSingleton<ITransport>(PostgreSqlTransport.TopicScheme,
            (sp, _) => sp.GetRequiredService<PostgreSqlTransport>());
        builder.Services.AddKeyedSingleton<ITransport>(PostgreSqlTransport.QueueScheme,
            (sp, _) => sp.GetRequiredService<PostgreSqlTransport>());
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
            // AddInterBus registers InterBusTopologyHostedService before transport configuration.
            // Insert migration hosted service before topology so schema exists for topology ensure calls.
            var descriptor = ServiceDescriptor.Singleton<IHostedService, PostgreSqlSchemaInitializer>();
            var topologyHostedServiceIndex = -1;
            for (var i = 0; i < builder.Services.Count; i++)
            {
                var service = builder.Services[i];
                if (service.ServiceType == typeof(IHostedService) &&
                    service.ImplementationType == typeof(InterBusTopologyHostedService))
                {
                    topologyHostedServiceIndex = i;
                    break;
                }
            }

            if (topologyHostedServiceIndex >= 0)
                builder.Services.Insert(topologyHostedServiceIndex, descriptor);
            else
                builder.Services.Add(descriptor);
        }

        switch (options.MessagePumpMode)
        {
            case PostgreSqlMessagePumpMode.Lease:
                builder.Services.AddHostedService<PostgreSqlLeaseMessagePump>();
                break;
            case PostgreSqlMessagePumpMode.Transactional:
                builder.Services.AddHostedService<PostgreSqlTransactionalMessagePump>();
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(options.MessagePumpMode), options.MessagePumpMode,
                    "Unsupported PostgreSQL message pump mode.");
        }
    }

    private static void ValidateOptions(PostgreSqlTransportOptions options)
    {
        if (options.LeaseDuration <= TimeSpan.Zero)
            throw new InvalidOperationException("PostgreSqlTransportOptions.LeaseDuration must be greater than zero.");

        if (options.LeaseRenewInterval is { } renewInterval && renewInterval <= TimeSpan.Zero)
            throw new InvalidOperationException(
                "PostgreSqlTransportOptions.LeaseRenewInterval must be greater than zero when set.");

        if (options.FailureRedeliveryDelay < TimeSpan.Zero)
            throw new InvalidOperationException(
                "PostgreSqlTransportOptions.FailureRedeliveryDelay cannot be negative.");
    }
}
