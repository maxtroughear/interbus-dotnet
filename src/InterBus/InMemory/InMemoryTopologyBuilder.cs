namespace InterBus.InMemory;

internal sealed class InMemoryTopologyBuilder(
    InMemoryBroker broker,
    InterBusOptions options)
    : ITopologyBuilder
{
    public Task BuildTopologyAsync(CancellationToken cancellationToken)
    {
        foreach (var descriptor in options.Consumers)
        {
            var queue = broker.EnsureQueue(descriptor);
            foreach (var type in descriptor.MessageTypes)
            {
                var messageType = options.GetPublisherDescriptor(type);

                broker.EnsureTopic(messageType.TopicName).AddSubscriber(queue);
            }
        }

        return Task.CompletedTask;
    }

    public Task<string> EnsureTopicAsync(Uri topic, CancellationToken cancellationToken)
    {
        broker.EnsureTopic(topic);
        return Task.FromResult(topic);
    }
}
