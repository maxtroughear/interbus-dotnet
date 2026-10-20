using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace InterBus.RabbitMQ;

/// <summary>
/// Singleton service that manages the <see cref="ConnectionFactory"/> configuration and
/// creates the long-lived connections used by the RabbitMQ transport:
/// <list type="bullet">
///   <item>A <em>publish</em> connection, lazily created on the first publish call and
///   shared for the lifetime of the application.</item>
///   <item>A <em>consume</em> connection, created once by the <see cref="RabbitMqMessagePump"/>
///   at startup.</item>
/// </list>
/// <para>
/// All connections are created with <see cref="ConnectionFactory.AutomaticRecoveryEnabled"/>
/// set to <c>true</c> so that the client automatically re-establishes connections and
/// re-subscribes consumers after transient broker failures.
/// </para>
/// </summary>
internal sealed class RabbitMqConnectionProvider : IAsyncDisposable
{
    private readonly RabbitMqTransportOptions _options;
    private readonly ILogger<RabbitMqConnectionProvider> _logger;

    private IConnection? _publishConnection;
    private readonly SemaphoreSlim _publishLock = new(1, 1);
    private bool _disposed;

    public RabbitMqConnectionProvider(RabbitMqTransportOptions options, ILogger<RabbitMqConnectionProvider> logger)
    {
        _options = options;
        _logger = logger;
    }

    /// <summary>
    /// Returns the shared publish connection, creating it on first use.
    /// Thread-safe via a semaphore; subsequent calls return the cached instance if open.
    /// </summary>
    public async Task<IConnection> GetPublishConnectionAsync(CancellationToken cancellationToken)
    {
        if (_publishConnection is { IsOpen: true })
            return _publishConnection;

        await _publishLock.WaitAsync(cancellationToken);
        try
        {
            if (_publishConnection is { IsOpen: true })
                return _publishConnection;

            if (_publishConnection is not null)
                await _publishConnection.DisposeAsync();

            _publishConnection = await CreateConnectionAsync(cancellationToken);
            _logger.LogDebug("InterBus RabbitMQ: publish connection established");
            return _publishConnection;
        }
        finally
        {
            _publishLock.Release();
        }
    }

    /// <summary>
    /// Creates a new dedicated connection for the message pump (consume side).
    /// The caller owns the connection and is responsible for disposing it.
    /// </summary>
    public Task<IConnection> CreateConsumeConnectionAsync(CancellationToken cancellationToken) =>
        CreateConnectionAsync(cancellationToken);

    private async Task<IConnection> CreateConnectionAsync(CancellationToken cancellationToken)
    {
        var factory = BuildFactory();
        return await factory.CreateConnectionAsync(cancellationToken);
    }

    /// <summary>
    /// Builds the <see cref="ConnectionFactory"/> using the configured options.
    /// <see cref="ConnectionFactory.AutomaticRecoveryEnabled"/> is always <c>true</c>.
    /// </summary>
    internal ConnectionFactory BuildFactory()
    {
        var factory = new ConnectionFactory
        {
            AutomaticRecoveryEnabled = true,
            NetworkRecoveryInterval = TimeSpan.FromSeconds(5),
        };

        if (!string.IsNullOrWhiteSpace(_options.Uri))
        {
            factory.Uri = new Uri(_options.Uri);
        }
        else
        {
            factory.HostName = _options.HostName;
            factory.Port = _options.Port;
            factory.VirtualHost = _options.VirtualHost;
            factory.UserName = _options.UserName;
            factory.Password = _options.Password;
        }

        return factory;
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        if (_publishConnection is not null)
        {
            await _publishConnection.CloseAsync();
            await _publishConnection.DisposeAsync();
        }

        _publishLock.Dispose();
    }
}
