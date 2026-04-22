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
                var topicName = options.TryGetTopicName(type, out var custom)
                    ? custom
                    : RoutingConventions.TopicName(type);
                broker.EnsureTopic(topicName).AddSubscriber(queue);
            }
        }

        return Task.CompletedTask;
    }

    public Task<string> EnsureTopicAsync(string rawTopicName, CancellationToken cancellationToken)
    {
        broker.EnsureTopic(rawTopicName);
        return Task.FromResult(rawTopicName);
    }

    public Task<string> EnsureQueueAsync(string rawQueueName, CancellationToken cancellationToken)
    {
        broker.TryEnsureQueue(rawQueueName);
        return Task.FromResult(rawQueueName);
    }
}
