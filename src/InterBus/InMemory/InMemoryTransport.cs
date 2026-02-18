using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Channels;

namespace InterBus.InMemory;

public class InMemoryTransport
{
    private ConcurrentDictionary<string, Channel<string>> _channels = new();

    public InMemoryTransport()
    {
        
    }

    public Task Publish(string topic, object message)
    {
        try
        {
            JsonSerializer.Serialize(message);
        }
    }

    public Task AddConsumer()
    {
        
    }
}