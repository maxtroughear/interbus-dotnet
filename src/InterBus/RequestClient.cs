using InterBus.Publishing;

namespace InterBus;

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
    IReplyEndpointFactory endpointFactory)
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
        await using var endpoint = await endpointFactory.CreateAsync<TReply>(cancellationToken);

        await publisher.PublishAsync(request, ctx =>
        {
            configure(ctx);
            ctx.Headers[InterBusHeaders.ReplyTo] = endpoint.Address.ToString();
        }, cancellationToken);

        return await endpoint.ReceiveAsync(cancellationToken);
    }
}
