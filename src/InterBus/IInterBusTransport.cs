namespace InterBus;

/// <summary>
/// Internal contract implemented by each transport. Used by the core <see cref="IMessagePublisher"/>
/// implementation to dispatch serialised messages without knowing the physical transport details.
/// Transport packages register their implementation of this interface via DI.
/// </summary>
public interface IInterBusTransport
{
    /// <summary>Publish a serialised payload to the named topic (fan-out to all subscriber queues).</summary>
    Task PublishToTopicAsync(string topicName, byte[] payload, MessageHeaders headers,
        CancellationToken cancellationToken);

    /// <summary>Send a serialised payload directly to the named queue (bypassing topic fan-out).</summary>
    Task SendToQueueAsync(string queueName, byte[] payload, MessageHeaders headers,
        CancellationToken cancellationToken);
}
