namespace InterBus;

/// <summary>
/// Runtime options populated by <see cref="IInterBusBuilder"/> during startup.
/// Consumed by transport implementations to initialise their topics and queues.
/// </summary>
public sealed class InterBusOptions
{
    private readonly List<ConsumerDescriptor> _consumers = [];

    /// <summary>All registered consumer descriptors.</summary>
    public IReadOnlyList<ConsumerDescriptor> Consumers => _consumers;

    internal void AddConsumer(ConsumerDescriptor descriptor) => _consumers.Add(descriptor);
}
