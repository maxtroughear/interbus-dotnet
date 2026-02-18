namespace InterBus;

public interface IConsumer
{
}

public interface IConsumer<T> : IConsumer where T : class
{
    Task Consume(T message);
}