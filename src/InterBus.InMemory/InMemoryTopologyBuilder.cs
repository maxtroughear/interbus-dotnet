namespace InterBus.InMemory;

internal sealed class InMemoryTopologyBuilder(
    InMemoryBroker broker,
    InterBusOptions options)
    : ITopologyBuilder
{
    public Task BuildTopologyAsync(CancellationToken cancellationToken)
    {
        foreach (var descriptor in options.Consumers)
            EnsureConsumerTopology(descriptor);

        return Task.CompletedTask;
    }

    public Task<string> EnsureTopicAsync(string rawTopicName, CancellationToken cancellationToken)
    {
        foreach (var descriptor in options.Consumers)
        {
            if (descriptor.MessageTypes.Any(type =>
                    (options.TryGetTopicName(type, out var custom) ? custom : RoutingConventions.TopicName(type)) ==
                    rawTopicName))
            {
                EnsureConsumerTopology(descriptor);
            }
        }

        return Task.FromResult(rawTopicName);
    }

    public Task<string> EnsureQueueAsync(string rawQueueName, CancellationToken cancellationToken)
    {
        broker.TryEnsureQueue(rawQueueName);
        return Task.FromResult(rawQueueName);
    }

    private void EnsureConsumerTopology(ConsumerDescriptor descriptor)
    {
        var queue = broker.EnsureQueue(descriptor);
        foreach (var type in descriptor.MessageTypes)
        {
            var topicName = options.TryGetTopicName(type, out var custom)
                ? custom
                : RoutingConventions.TopicName(type);
            broker.EnsureTopic(topicName).AddSubscriber(queue);
        }
    }
}
