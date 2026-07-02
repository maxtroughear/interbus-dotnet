using System.Collections.Concurrent;
using System.Threading.Channels;
using Azure.Messaging.ServiceBus;
using InterBus.Requests;
using Microsoft.Extensions.Logging;

namespace InterBus.AzureServiceBus;

/// <summary>
/// Singleton that manages a single shared, session-enabled Azure Service Bus queue used for
/// all request/reply exchanges in the current application instance.
/// <para>
/// Each call to <see cref="RegisterAsync"/> generates a unique virtual queue ID and allocates an
/// in-memory <see cref="Channel{T}"/> for that session.  A background
/// <see cref="ServiceBusSessionProcessor"/> polls the host queue and routes incoming reply
/// messages to the correct channel by their <see cref="ServiceBusReceivedMessage.SessionId"/>.
/// </para>
/// <para>
/// This avoids per-request queue creation/deletion latency while preserving strict reply
/// isolation — the virtual queue ID is encoded in the <c>?vq=</c> query parameter of the
/// <see cref="IReplyEndpoint.Address"/> URI so that the sending transport sets
/// <see cref="ServiceBusMessage.SessionId"/> on the outgoing reply.
/// </para>
/// </summary>
internal sealed class AzureServiceBusVirtualQueueHost(
    AzureServiceBusClientProvider clientProvider,
    AzureServiceBusTopologyBuilder topology,
    AzureServiceBusTransportOptions options,
    ILogger<AzureServiceBusVirtualQueueHost> logger)
    : IVirtualQueueHost, IAsyncDisposable
{
    private readonly SemaphoreSlim _initLock = new(1, 1);

    private readonly ConcurrentDictionary<string, Channel<(byte[] Payload, MessageHeaders Headers)>>
        _channels = new(StringComparer.Ordinal);

    private string? _rawQueueName;
    private string? _azureQueueName;
    private ServiceBusSessionProcessor? _processor;

    /// <summary>
    /// Lazily creates the shared virtual queue host (once) then allocates a new in-memory
    /// channel keyed by a freshly generated virtual queue ID.
    /// </summary>
    /// <returns>
    /// The raw reply queue name (no prefix, for use in URIs), the new virtual queue ID, and a reader
    /// end of the channel that will receive exactly one reply payload.
    /// </returns>
    public async
        Task<VirtualQueueRegistration> RegisterAsync(CancellationToken cancellationToken = default)
    {
        await EnsureHostQueueAsync(cancellationToken);

        var virtualQueueId = Guid.CreateVersion7().ToString("N");
        var channel = Channel.CreateUnbounded<(byte[], MessageHeaders)>(
            new UnboundedChannelOptions { SingleReader = true });
        _channels[virtualQueueId] = channel;

        return new VirtualQueueRegistration(_rawQueueName!, virtualQueueId, channel.Reader);
    }

    /// <summary>Completes and removes the channel for <paramref name="virtualQueueId"/>.</summary>
    public void Unregister(string virtualQueueId)
    {
        if (_channels.TryRemove(virtualQueueId, out var channel))
            channel.Writer.TryComplete();
    }

    private async Task EnsureHostQueueAsync(CancellationToken cancellationToken)
    {
        if (_azureQueueName is not null)
            return;

        await _initLock.WaitAsync(cancellationToken);
        try
        {
            if (_azureQueueName is not null)
                return;

            var rawName = $"InterBus.Reply.{Guid.CreateVersion7():N}";
            var azureQueueName = await topology.EnsureSessionQueueAsync(rawName, cancellationToken);
            _rawQueueName = rawName;
            _azureQueueName = azureQueueName;

            var processorOptions = new ServiceBusSessionProcessorOptions
            {
                MaxConcurrentSessions = options.ReplyMaxConcurrentSessions,
                MaxConcurrentCallsPerSession = 1,
                SessionIdleTimeout = options.ReplySessionIdleTimeout,
                AutoCompleteMessages = false,
                ReceiveMode = ServiceBusReceiveMode.PeekLock,
            };

            _processor = clientProvider.CreateQueueSessionProcessor(azureQueueName, processorOptions);
            _processor.ProcessMessageAsync += HandleReplyAsync;
            _processor.ProcessErrorAsync += HandleErrorAsync;
            await _processor.StartProcessingAsync(CancellationToken.None);

            logger.LogDebug("InterBus AzureServiceBus: created virtual queue host {QueueName}", azureQueueName);
        }
        finally
        {
            _initLock.Release();
        }
    }

    private async Task HandleReplyAsync(ProcessSessionMessageEventArgs args)
    {
        var sessionId = args.SessionId;

        if (!_channels.TryGetValue(sessionId, out var channel))
        {
            // No listener registered for this session — complete to avoid redelivery.
            logger.LogWarning(
                "InterBus AzureServiceBus: received reply for unknown session {SessionId}; discarding",
                sessionId);
            await args.CompleteMessageAsync(args.Message, CancellationToken.None);
            return;
        }

        try
        {
            var headers = ParseApplicationProperties(args.Message.ApplicationProperties);
            channel.Writer.TryWrite((args.Message.Body.ToArray(), headers));
            await args.CompleteMessageAsync(args.Message, CancellationToken.None);
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "InterBus AzureServiceBus: failed to route reply for session {SessionId}; abandoning",
                sessionId);
            await args.AbandonMessageAsync(args.Message, cancellationToken: CancellationToken.None);
        }
    }

    private Task HandleErrorAsync(ProcessErrorEventArgs args)
    {
        logger.LogError(args.Exception,
            "InterBus AzureServiceBus: error on virtual queue host processor; source={ErrorSource}",
            args.ErrorSource);
        return Task.CompletedTask;
    }

    private static MessageHeaders ParseApplicationProperties(
        IReadOnlyDictionary<string, object?> applicationProperties)
    {
        var dict = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var kv in applicationProperties)
        {
            var value = kv.Value switch
            {
                string str => str,
                _ => kv.Value?.ToString(),
            };
            if (value is not null)
                dict[kv.Key] = value;
        }

        return new MessageHeaders(dict);
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (_processor is not null)
        {
            try
            {
                await _processor.StopProcessingAsync(CancellationToken.None);
                await _processor.DisposeAsync();
            }
            catch
            {
                // Ignored on disposal.
            }
        }

        if (_azureQueueName is not null)
        {
            try
            {
                await clientProvider.AdminClient.DeleteQueueAsync(_azureQueueName!, CancellationToken.None);
            }
            catch
            {
                logger.LogWarning("InterBus AzureServiceBus: failed to delete virtual queue host {QueueName}",
                    _azureQueueName);
            }
        }

        foreach (var channel in _channels.Values)
            channel.Writer.TryComplete();
        _channels.Clear();

        _initLock.Dispose();
    }
}
