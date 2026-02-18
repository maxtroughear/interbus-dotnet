using System.Collections.Concurrent;

namespace InterBus.InMemory;

public class InMemoryMessagePublisher : IMessagePublisher
{
    // Reference to the in memory bus
    private readonly InMemoryBus _bus;

    public InMemoryMessagePublisher(InMemoryBus bus)
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

public class InMemoryTransport2 : IMessagePublisher
{
    // Key is the topic, value is any subscribed queues

    private readonly ConcurrentDictionary<Type, string> _topicTypeMap = new();

    private readonly ConcurrentDictionary<string, InMemoryTopic> _topics = new();
    // private readonly ConcurrentDictionary<string, ConcurrentBag<InMemoryQueue>> _queuesByTopic = new();
    // private readonly ConcurrentDictionary<string, InMemoryQueue> _queues = new();

    public Task Publish<T>(T message)
    {
        var topic = GetTopic(typeof(T));


        // _topics.FirstOrDefault(x => x.Value.Type == typeof(T));
        // _topics.TryGetValue(typeof(T), out var queue);
        
        return Task.CompletedTask;
    }

    public void RegisterTopic<T>()
    {
        RegisterTopic(typeof(T));
    }

    public void RegisterTopic<T>(string topicName)
    {
        RegisterTopic(typeof(T), topicName);
    }

    private InMemoryTopic RegisterTopic(Type message)
    {
        return message.FullName is null
            ? throw new ArgumentOutOfRangeException(nameof(message), "Message type must have a full name")
            : RegisterTopic(message, message.FullName);
    }

    private InMemoryTopic RegisterTopic(Type message, string topicName)
    {
        if (!_topicTypeMap.TryAdd(message, topicName) && _topicTypeMap[message] != topicName)
        {
            throw new InvalidOperationException("Topic type already registered with a different name");
        }

        if (_topics.TryGetValue(topicName, out var topic)) return topic;

        topic = new InMemoryTopic(message, topicName);
        _topics.TryAdd(topicName, topic);
        return topic;
    }

    private InMemoryTopic GetTopic(Type message)
    {
        if (!_topicTypeMap.TryGetValue(message, out var topicName))
        {
            topicName = RegisterTopic(message).Name;
        }

        return _topics[topicName];
    }

    public void AddConsumer<TConsumer>() where TConsumer : IConsumer
    {
        var consumerInterfaces = typeof(TConsumer).GetInterfaces().Where(x => x == typeof(IConsumer<>));
        foreach (var consumer in consumerInterfaces)
        {
            var messageType = consumer.GetGenericArguments().Single();
            AddConsumer(typeof(TConsumer), messageType);
        }
    }

    public void AddConsumer<TConsumer, TMessage>() where TConsumer : IConsumer<TMessage> where TMessage : class
    {
        AddConsumer(typeof(TConsumer), typeof(TMessage));
    }

    private void AddConsumer(Type consumer, Type message)
    {
        RegisterTopic(message);
        var topic = GetTopic(message);

        if (consumer.FullName is null) return;

        var queue = new InMemoryQueue(consumer.FullName, consumer);
        topic.AddQueue(queue, consumer.FullName);
    }
}

public class InMemoryTopic
{
    public string Name { get; }
    private readonly Type _type;
    public ConcurrentDictionary<string, InMemoryQueue> Queues { get; } = new();

    public InMemoryTopic(Type type, string name)
    {
        _type = type;
        Name = name;
    }

    public void AddQueue(InMemoryQueue queue, string name)
    {
        Queues.TryAdd(name, queue);
    }
}

public class InMemoryQueue
{
    private readonly string _name;
    private readonly Type _consumerType;
    private readonly ConcurrentQueue<object> _messages = [];

    public InMemoryQueue(string name, Type consumerType)
    {
        _name = name;
        _consumerType = consumerType;
    }
}

public record Event(string Text);

public class EventConsumer : IConsumer<Event>
{
    public Task Consume(Event message)
    {
        Console.WriteLine(message.Text);
        return Task.CompletedTask;
    }
}

public class Example
{
    public Example()
    {
        var bus = new InMemoryTransport2();

        bus.RegisterTopic<Event>();
        bus.AddConsumer<EventConsumer>();
    }
}