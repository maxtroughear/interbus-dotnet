using InterBus.Sending;

namespace InterBus.Hangfire;

/// <summary>
/// Hangfire job handler for all scheduled InterBus messages (both topic fan-out and
/// point-to-point queue send). Resolved from DI via constructor injection.
/// </summary>
public sealed class HangfireDeliverFunction(SendPipelineExecutor sendPipeline)
{
    public Task InvokeAsync(ScheduledDeliveryRequest request)
    {
        var destination = new Uri(request.Destination);
        var headers = new MessageHeaders(request.Headers);
        var sendContext = new SendContext(destination, request.Payload, headers, delay: null);
        return sendPipeline.ExecuteAsync(sendContext, cancellationToken: CancellationToken.None);
    }
}
