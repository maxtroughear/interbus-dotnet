using InterBus.Consumer;
using InterBus.Send;

namespace InterBus.DeadLettering;

/// <summary>
/// Outermost consume middleware that marks unhandled consumer exceptions for dead-lettering
/// and sends a <see cref="DeadLetter{T}"/> message to allow application consumers to
/// receive and process dead-lettered messages.
/// <para>
/// The dead-letter notification is sent directly to the transport via
/// <see cref="SendPipelineExecutor"/>, bypassing the publish middleware pipeline (e.g. the
/// transactional outbox). This guarantees the notification reaches the transport even when
/// the consumer's outbox middleware would otherwise intercept and defer it — in the failure
/// path no <c>SaveChangesAsync</c> is ever called, so an outbox-routed publish would be
/// silently lost.
/// </para>
/// </summary>
public sealed class DeadLetterConsumeMiddleware(
    SendPipelineExecutor sendPipeline,
    IMessageSerializer serializer,
    InterBusOptions options,
    ConsumeContextAccessor contextAccessor) : IConsumeMiddleware
{
    public async Task InvokeAsync<T>(
        ConsumeContext<T> context,
        Func<ConsumeContext<T>, CancellationToken, Task> next,
        CancellationToken cancellationToken = default) where T : class
    {
        try
        {
            await next(context, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (DeadLetterException)
        {
            throw;
        }
        catch (Exception ex)
        {
            var deadLetteredAtUtc = DateTimeOffset.UtcNow;
            const string reason = "Unhandled exception in consume pipeline";

            context.Headers[InterBusHeaders.DeadLetterReason] = reason;
            context.Headers[InterBusHeaders.DeadLetterError] = ex.ToString();
            context.Headers[InterBusHeaders.DeadLetteredAt] = deadLetteredAtUtc.ToString("O");

            // Send a DeadLetter<T> event directly to the transport, bypassing publish middleware
            // (e.g. the transactional outbox) so the notification always reaches the transport
            // regardless of the consumer's outbox configuration.
            // Skip if T is already a DeadLetter<> type to prevent infinite nesting.
            if (!typeof(T).IsGenericType || typeof(T).GetGenericTypeDefinition() != typeof(DeadLetter<>))
            {
                var deadLetter = new DeadLetter<T>
                {
                    OriginalMessage = context.Message,
                    Reason = reason,
                    Error = ex.ToString(),
                    DeadLetteredAtUtc = deadLetteredAtUtc,
                };

                try
                {
                    // Use CancellationToken.None so a cancelled consumer token does not suppress
                    // the dead-letter notification.
                    await SendDeadLetterDirectAsync(deadLetter, CancellationToken.None);
                }
                catch
                {
                    // Swallow send errors to avoid masking the original consumer exception.
                }
            }

            throw new DeadLetterException(reason, ex.ToString(), deadLetteredAtUtc, ex);
        }
    }

    private Task SendDeadLetterDirectAsync<T>(T deadLetter, CancellationToken cancellationToken) where T : class
    {
        var topicName = options.TryGetTopicName(typeof(T), out var customName)
            ? customName
            : RoutingConventions.TopicName(typeof(T));
        var destination = RoutingConventions.TopicUri(options.TopicUriScheme, topicName);

        var consumeHeaders = contextAccessor.ConsumeContext?.Headers;

        string? correlationId = null;
        string? causationId = null;
        consumeHeaders?.TryGetValue(InterBusHeaders.CorrelationId, out correlationId);
        consumeHeaders?.TryGetValue(InterBusHeaders.MessageId, out causationId);

        var headers = new MessageHeaders
        {
            [InterBusHeaders.MessageType] = RoutingConventions.TopicName(typeof(T)),
            [InterBusHeaders.MessageId] = Guid.CreateVersion7().ToString(),
            [InterBusHeaders.CorrelationId] = correlationId ?? Guid.CreateVersion7().ToString(),
            [InterBusHeaders.SentTime] = DateTimeOffset.UtcNow.ToString("O"),
        };

        if (causationId is not null)
            headers[InterBusHeaders.CausationId] = causationId;

        var payload = serializer.Serialize(deadLetter);
        var sendContext = new SendContext(destination, payload, headers);
        return sendPipeline.ExecuteAsync(sendContext, cancellationToken: cancellationToken);
    }
}
