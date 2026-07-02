using System.Threading.Channels;

namespace InterBus.Requests;

/// <summary>
/// Transport-specific host for virtual request/reply queues.
/// </summary>
public interface IVirtualQueueHost
{
    /// <summary>
    /// Creates a new virtual queue registration that can receive one reply.
    /// </summary>
    Task<VirtualQueueRegistration> RegisterAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes a previously created virtual queue registration.
    /// </summary>
    void Unregister(string virtualQueueId);
}

/// <summary>
/// Registration details for a transport-managed virtual reply queue.
/// </summary>
/// <param name="QueueName">The raw queue name used to construct the reply address.</param>
/// <param name="VirtualQueueId">The transport-specific virtual queue identifier.</param>
/// <param name="Reader">Message stream for the virtual queue.</param>
public readonly record struct VirtualQueueRegistration(
    string QueueName,
    string VirtualQueueId,
    ChannelReader<(byte[] Payload, MessageHeaders Headers)> Reader);
