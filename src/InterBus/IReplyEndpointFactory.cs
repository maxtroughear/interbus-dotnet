namespace InterBus;

/// <summary>
/// Transport-specific factory that creates temporary reply endpoints for
/// <see cref="IRequestClient{TRequest,TReply}"/> exchanges.
/// Each transport that supports request/reply must register an implementation of this interface.
/// </summary>
public interface IReplyEndpointFactory
{
    /// <summary>
    /// Creates a new temporary <see cref="IReplyEndpoint"/> that is ready to receive
    /// a single reply. The caller must dispose the endpoint after the exchange is complete.
    /// </summary>
    Task<IReplyEndpoint> CreateAsync(CancellationToken cancellationToken = default);
}
