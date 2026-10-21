using InterBus.Sending;

namespace InterBus.Tests.Common;

public sealed class SequenceTraceSendMiddleware(SendSequenceTracker tracker) : ISendMiddleware
{
    public const string SequenceHeaderName = "x-interbus-test-sequence";
    public async Task InvokeAsync(SendContext context, Func<SendContext, CancellationToken, Task> next, CancellationToken cancellationToken = default)
    {
        if (context.Headers.TryGetValue(InterBusHeaders.MessageGroupId, out var messageGroupId) && context.Headers.TryGetValue(SequenceHeaderName, out var sequenceHeader) && int.TryParse(sequenceHeader, out var sequence))
            tracker.Track(messageGroupId, sequence);
        await next(context, cancellationToken);
    }
}
