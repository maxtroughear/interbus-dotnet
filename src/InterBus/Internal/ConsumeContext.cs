using InterBus.Middleware;

namespace InterBus.Internal;

internal sealed class ConsumeContext(MessageHeaders headers) : IConsumeContext
{
    public IReadOnlyDictionary<string, string> Headers { get; } = headers;
    public string? CorrelationId { get; } = headers.GetValueOrDefault(InterBusHeaders.CorrelationId);
    public string MessageType { get; } = headers[InterBusHeaders.MessageType];

    public Task Publish<T>(T message, CancellationToken cancellationToken = default) where T : class
    {
        throw new NotImplementedException();
    }

    public Task Publish<T>(T message, Action<PublishContext<T>>? configure = null,
        CancellationToken cancellationToken = default) where T : class
    {
        throw new NotImplementedException();
    }

    public Task Send<TConsumer, T>(T message, CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T> where T : class
    {
        throw new NotImplementedException();
    }

    public Task Send<TConsumer, T>(T message, Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default) where TConsumer : IConsumer<T> where T : class
    {
        throw new NotImplementedException();
    }

    public Task Send<T>(string queueName, T message, CancellationToken cancellationToken = default) where T : class
    {
        throw new NotImplementedException();
    }

    public Task Send<T>(string queueName, T message, Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default) where T : class
    {
        throw new NotImplementedException();
    }
}
