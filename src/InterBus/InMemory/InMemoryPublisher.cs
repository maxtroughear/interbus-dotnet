using System.Collections.Concurrent;

namespace InterBus.InMemory;

public class InMemoryPublisher : IPublisher
{
    // Reference to the in memory bus
    private readonly InMemoryBus _bus;

    public InMemoryPublisher(InMemoryBus bus)
    {
        _bus = bus;
    }

    public Task Publish<T>(T message)
    {
        // TODO: Route from message type to topic
        // TODO: Route from topic to queue
        throw new NotImplementedException();
    }
}

public class InMemoryBus
{
    private ConcurrentDictionary<string, ConcurrentQueue<object>> _messages = new();
}