using System.Diagnostics;
using InterBus.Publishing;
using InterBus.Telemetry;

namespace InterBus.Requests;

/// <summary>
/// Core implementation of <see cref="IRequestClient{TRequest,TReply}"/>.
/// Publishes the request to the topic for <typeparamref name="TRequest"/> with the
/// <see cref="InterBusHeaders.ReplyTo"/> header set to a temporary reply endpoint address,
/// then waits for a reply of type <typeparamref name="TReply"/> on that endpoint.
/// The endpoint is created by the transport's <see cref="IReplyEndpointFactory"/> and is
/// disposed automatically when the exchange completes.
/// </summary>
internal sealed class RequestClient<TRequest, TReply>(
    IPublisher publisher,
    IReplyEndpointFactory endpointFactory,
    IInterBusDiagnostics diagnostics,
    InterBusMetrics metrics)
    : IRequestClient<TRequest, TReply>
    where TRequest : class
    where TReply : class
{
    public Task<TReply> RequestAsync(TRequest request, CancellationToken cancellationToken = default) =>
        RequestAsync(request, _ => { }, cancellationToken);

    public async Task<TReply> RequestAsync(
        TRequest request,
        Action<PublishContext<TRequest>> configure,
        CancellationToken cancellationToken = default)
    {
        await using var endpoint = await endpointFactory.CreateAsync(cancellationToken);
        var requestType = RoutingConventions.MessageTypeString(typeof(TRequest));
        var replyType = RoutingConventions.MessageTypeString(typeof(TReply));
        var destinationName = RoutingConventions.TopicName(typeof(TRequest));
        var replyDestinationName = RoutingConventions.GetDestinationName(endpoint.Address);
        var startedAt = Stopwatch.GetTimestamp();

        using var activity = diagnostics.ActivitySource.StartActivity($"{requestType} request", ActivityKind.Client);
        activity?.SetTag("messaging.system", "interbus");
        activity?.SetTag("messaging.operation.type", "request");
        activity?.SetTag("messaging.destination.name", destinationName);
        activity?.SetTag("messaging.destination.reply.name", replyDestinationName);
        activity?.SetTag("messaging.message.type", requestType);
        activity?.SetTag("messaging.message.reply_type", replyType);
        activity?.AddEvent(new ActivityEvent("interbus.request.started"));
        metrics.RecordRequestStarted(requestType, replyType, destinationName);

        try
        {
            await publisher.PublishAsync(request, ctx =>
            {
                configure(ctx);
                ctx.Headers[InterBusHeaders.ReplyTo] = endpoint.Address.ToString();
            }, cancellationToken);

            activity?.AddEvent(new ActivityEvent("interbus.request.published"));

            var reply = await endpoint.ReceiveAsync<TReply>(cancellationToken);
            activity?.SetStatus(ActivityStatusCode.Ok);
            activity?.AddEvent(new ActivityEvent("interbus.reply.received"));
            metrics.RecordRequestCompleted(requestType, replyType, destinationName);
            return reply;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.AddEvent(new ActivityEvent("interbus.request.failed",
                tags: new ActivityTagsCollection
                {
                    { "exception.type", ex.GetType().FullName },
                    { "exception.message", ex.Message },
                }));
            metrics.RecordRequestFailed(requestType, replyType, destinationName);
            throw;
        }
        finally
        {
            metrics.RecordRequestDuration(
                Stopwatch.GetElapsedTime(startedAt).TotalSeconds,
                requestType,
                replyType,
                destinationName);
        }
    }
}
