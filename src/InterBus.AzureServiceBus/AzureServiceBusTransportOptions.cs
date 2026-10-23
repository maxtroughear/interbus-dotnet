using TokenCredential = Azure.Core.TokenCredential;
using InterBus.Retry;

namespace InterBus.AzureServiceBus;

/// <summary>
/// Configuration options for the Azure Service Bus transport.
/// </summary>
public sealed class AzureServiceBusTransportOptions
{
    /// <summary>
    /// Namespace host name, such as <c>example.servicebus.windows.net</c>, used with
    /// <see cref="Credential"/> for Microsoft Entra authentication. When set, both SDK clients
    /// use this namespace instead of the connection strings.
    /// </summary>
    public string? FullyQualifiedNamespace { get; set; }

    /// <summary>
    /// Credential used with <see cref="FullyQualifiedNamespace"/> for messaging and entity administration.
    /// Required when a namespace host name is configured. The host owns credential lifetime.
    /// </summary>
    public TokenCredential? Credential { get; set; }

    /// <summary>
    /// The Azure Service Bus namespace connection string. Required unless using
    /// <see cref="FullyQualifiedNamespace"/> and <see cref="Credential"/>.
    /// Used by <see cref="Azure.Messaging.ServiceBus.ServiceBusClient"/> for publishing and consuming messages.
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>
    /// Optional connection string for the
    /// <see cref="Azure.Messaging.ServiceBus.Administration.ServiceBusAdministrationClient"/>
    /// used to declare topics, queues, and subscriptions at startup.
    /// When left <see langword="null"/> (the default), <see cref="ConnectionString"/> is used.
    /// <para>
    /// Set this when the administration endpoint differs from the messaging endpoint, for
    /// example when using the Azure Service Bus Emulator via Testcontainers where
    /// <c>ServiceBusContainer.GetHttpConnectionString()</c> returns the HTTP management URL.
    /// </para>
    /// </summary>
    public string? AdminConnectionString { get; set; }

    /// <summary>
    /// Optional prefix applied to Azure Service Bus topic names.
    /// Defaults to empty string (no prefix).
    /// </summary>
    public string TopicPrefix { get; set; } = string.Empty;

    /// <summary>
    /// Optional prefix applied to Azure Service Bus queue names.
    /// Defaults to empty string (no prefix).
    /// </summary>
    public string QueuePrefix { get; set; } = string.Empty;

    /// <summary>
    /// Configures retry behavior for connection-related failures during message pump startup.
    /// Defaults to effectively unbounded retries with exponential backoff from 1 second up to
    /// 30 seconds.
    /// </summary>
    public RetryOptions ConnectionRetry { get; set; } = new()
    {
        RetryCount = int.MaxValue,
        DelayProvider = RetryDelays.Exponential(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30)),
    };

    /// <summary>
    /// Maximum number of concurrent reply sessions. Effectively limits the number of concurrent replies that
    /// can be handled.
    /// </summary>
    public int ReplyMaxConcurrentSessions { get; set; } = 50;

    /// <summary>
    /// The maximum amount of time to wait for a message to be received for the currently active session.
    /// After this time has elapsed, the processor will close the session and attempt to process another session.
    /// </summary>
    public TimeSpan ReplySessionIdleTimeout { get; set; } = TimeSpan.FromSeconds(5);
}
