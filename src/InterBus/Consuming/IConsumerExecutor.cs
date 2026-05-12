namespace InterBus.Consuming;

public interface IConsumerExecutor<TConsumer, TMessage>
    where TConsumer : IConsume<TMessage>
    where TMessage : class
{
    Task ExecuteAsync(ConsumeContext<TMessage> context, CancellationToken cancellationToken = default);
}
