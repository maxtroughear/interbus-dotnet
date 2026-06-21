namespace InterBus.InMemory;

internal sealed class InMemoryTopologyBuilder(
    InMemoryBroker broker,
    InterBusOptions options)
    : ITopologyBuilder
{
    public Task BuildConsumerTopologyAsync(CancellationToken cancellationToken)
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
        var topicName = RoutingConventions.GetDestinationName(topic);
        broker.EnsureTopic(topicName);
        return Task.FromResult(topicName);
    }
}
