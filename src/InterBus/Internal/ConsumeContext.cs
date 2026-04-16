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

    public Task PublishAsync<T>(T message, CancellationToken cancellationToken = default) where T : class =>
        publisher.PublishAsync(message, cancellationToken);

    public Task PublishAsync<T>(T message, Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default) where T : class =>
        publisher.PublishAsync(message, configure, cancellationToken);

    public Task SendAsync<TConsumer, T>(T message, CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T> where T : class =>
        publisher.SendAsync<TConsumer, T>(message, cancellationToken);

    public Task SendAsync<TConsumer, T>(T message, Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default) where TConsumer : IConsumer<T> where T : class =>
        publisher.SendAsync<TConsumer, T>(message, configure, cancellationToken);

    public Task SendAsync<T>(string queueName, T message, CancellationToken cancellationToken = default) where T : class =>
        publisher.SendAsync(queueName, message, cancellationToken);

    public Task SendAsync<T>(string queueName, T message, Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default) where T : class =>
        publisher.SendAsync(queueName, message, configure, cancellationToken);

    public Task SchedulePublishAsync<T>(T message, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default) where T : class =>
        RequireScheduler().SchedulePublishAsync(message, scheduledAt, cancellationToken);

    public Task SchedulePublishAsync<T>(T message, DateTimeOffset scheduledAt, Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default) where T : class =>
        RequireScheduler().SchedulePublishAsync(message, scheduledAt, configure, cancellationToken);

    public Task ScheduleSendAsync<TConsumer, T>(T message, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T> where T : class =>
        RequireScheduler().ScheduleSendAsync<TConsumer, T>(message, scheduledAt, cancellationToken);

    public Task ScheduleSendAsync<TConsumer, T>(T message, DateTimeOffset scheduledAt,
        Action<PublishContext<T>> configure, CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T> where T : class =>
        RequireScheduler().ScheduleSendAsync<TConsumer, T>(message, scheduledAt, configure, cancellationToken);

    public Task ScheduleSendAsync<T>(string queueName, T message, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default) where T : class =>
        RequireScheduler().ScheduleSendAsync(queueName, message, scheduledAt, cancellationToken);

    public Task ScheduleSendAsync<T>(string queueName, T message, DateTimeOffset scheduledAt,
        Action<PublishContext<T>> configure, CancellationToken cancellationToken = default) where T : class =>
        RequireScheduler().ScheduleSendAsync(queueName, message, scheduledAt, configure, cancellationToken);

    private IMessageScheduler RequireScheduler() =>
        scheduler ?? throw new InvalidOperationException(
            "Scheduling is not configured or no IMessageSchedulingProvider is available in the current scope. " +
            "Call AddScheduling() or AddPublishScheduling() on the InterBus builder and ensure an " +
            "IMessageSchedulingProvider is registered (for example via a scheduling transport package).");
}
