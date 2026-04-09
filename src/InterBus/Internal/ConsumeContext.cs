using InterBus.Middleware;

namespace InterBus.Internal;

internal sealed class ConsumeContext(MessageHeaders headers, IMessagePublisher publisher) : IConsumeContext
{
    public IReadOnlyDictionary<string, string> Headers { get; } = headers;
    public string? CorrelationId { get; } = headers.GetValueOrDefault(InterBusHeaders.CorrelationId);
    public string MessageType { get; } = headers[InterBusHeaders.MessageType];

    public Task Publish<T>(T message, CancellationToken cancellationToken = default) where T : class =>
        publisher.Publish(message, cancellationToken);

    public Task Publish<T>(T message, Action<PublishContext<T>>? configure = null,
        CancellationToken cancellationToken = default) where T : class =>
        configure is null
            ? publisher.Publish(message, cancellationToken)
            : publisher.Publish(message, configure, cancellationToken);

    public Task Send<TConsumer, T>(T message, CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T> where T : class =>
        publisher.Send<TConsumer, T>(message, cancellationToken);

    public Task Send<TConsumer, T>(T message, Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default) where TConsumer : IConsumer<T> where T : class =>
        publisher.Send<TConsumer, T>(message, configure, cancellationToken);

    public Task Send<T>(string queueName, T message, CancellationToken cancellationToken = default) where T : class =>
        publisher.Send(queueName, message, cancellationToken);

    public Task Send<T>(string queueName, T message, Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default) where T : class =>
        publisher.Send(queueName, message, configure, cancellationToken);
}
