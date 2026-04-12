using InterBus.Middleware;

namespace InterBus.Internal;

internal sealed class ConsumeContext(
    MessageHeaders headers,
    IMessagePublisher publisher,
    IMessageScheduler? scheduler) : IConsumeContext
{
    public IReadOnlyDictionary<string, string> Headers { get; } = headers;
    public string? CorrelationId { get; } = headers.GetValueOrDefault(InterBusHeaders.CorrelationId);
    public string MessageType { get; } = headers[InterBusHeaders.MessageType];

    public Task Publish<T>(T message, CancellationToken cancellationToken = default) where T : class =>
        publisher.Publish(message, cancellationToken);

    public Task Publish<T>(T message, Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default) where T : class =>
        publisher.Publish(message, configure, cancellationToken);

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

    public Task SchedulePublish<T>(T message, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default) where T : class =>
        RequireScheduler().SchedulePublish(message, scheduledAt, cancellationToken);

    public Task SchedulePublish<T>(T message, DateTimeOffset scheduledAt, Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default) where T : class =>
        RequireScheduler().SchedulePublish(message, scheduledAt, configure, cancellationToken);

    public Task ScheduleSend<TConsumer, T>(T message, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T> where T : class =>
        RequireScheduler().ScheduleSend<TConsumer, T>(message, scheduledAt, cancellationToken);

    public Task ScheduleSend<TConsumer, T>(T message, DateTimeOffset scheduledAt,
        Action<PublishContext<T>> configure, CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T> where T : class =>
        RequireScheduler().ScheduleSend<TConsumer, T>(message, scheduledAt, configure, cancellationToken);

    public Task ScheduleSend<T>(string queueName, T message, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default) where T : class =>
        RequireScheduler().ScheduleSend(queueName, message, scheduledAt, cancellationToken);

    public Task ScheduleSend<T>(string queueName, T message, DateTimeOffset scheduledAt,
        Action<PublishContext<T>> configure, CancellationToken cancellationToken = default) where T : class =>
        RequireScheduler().ScheduleSend(queueName, message, scheduledAt, configure, cancellationToken);

    private IMessageScheduler RequireScheduler() =>
        scheduler ?? throw new InvalidOperationException(
            "Scheduling is not configured or no IMessageSchedulingProvider is available in the current scope. " +
            "Call AddScheduling() or AddPublishScheduling() on the InterBus builder and ensure an " +
            "IMessageSchedulingProvider is registered (for example via a scheduling transport package).");
}
