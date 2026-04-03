namespace InterBus.RabbitMQ;

/// <summary>
/// Per-queue RabbitMQ options configured at registration time.
/// </summary>
public sealed class RabbitMqQueueOptions
{
    /// <summary>
    /// Number of concurrent message handlers dispatched from a single consumer channel.
    /// Maps to <c>ConsumerDispatchConcurrency</c> on the channel's
    /// <see cref="RabbitMQ.Client.CreateChannelOptions"/>.
    /// Defaults to <c>1</c> (sequential per-channel dispatch).
    /// </summary>
    public ushort ConcurrencyCount { get; set; } = 1;
}
