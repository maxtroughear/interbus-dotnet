namespace InterBus.Publishing;

public sealed class PublisherDescriptor(Type messageType, Uri topic)
{
    public Type MessageType { get; } = messageType;
    
    public Uri Topic { get; internal set; } = topic;
    
    public string TopicName => RoutingConventions.GetDestinationName(Topic);
    
    public bool IsFifo { get; internal set; }
    
    public Func<object, string>? GroupBy { get; internal set; }
}
