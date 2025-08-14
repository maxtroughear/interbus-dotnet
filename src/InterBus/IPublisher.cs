namespace InterBus;

public interface IPublisher
{
    // TODO: publish options? message ID setting, Correlation ID overriding, etc.
    public Task Publish<T>(T message);
}