using InterBus.Dispatching;

namespace InterBus.MessagePumps;

/// <summary>An owned transport delivery. Ownership lasts through settlement and asynchronous disposal.</summary>
/// <remarks>Settlement returns false when ownership was rejected. Exceptions may indicate ambiguous settlement;
/// callers must not attempt a conflicting settlement. Disposal stops renewal and releases local resources,
/// allowing broker recovery if settlement did not complete. Cancellation never transfers ownership.</remarks>
public interface IDelivery : IAsyncDisposable
{
    /// <summary>Decoded payload, or an exception when envelope decoding failed.</summary>
    ReadOnlyMemory<byte> Payload { get; }

    /// <summary>Decoded message headers.</summary>
    MessageHeaders Headers { get; }

    /// <summary>Envelope decoding failure, if any. The delivery still owns its transport resources.</summary>
    Exception? DecodeError => null;

    /// <summary>Optional key serialized in handoff order through settlement and cleanup.</summary>
    string? OrderingKey => null;

    /// <summary>Settles the delivery once. The token bounds transport operations; false means ownership was lost.</summary>
    Task<bool> SettleAsync(DispatchResult result, CancellationToken cancellationToken);
}
