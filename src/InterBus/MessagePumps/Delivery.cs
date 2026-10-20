using InterBus.Consuming;
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

/// <summary>Runs the shared scoped dispatch pipeline and settles an owned delivery.</summary>
public sealed class DeliveryProcessor(Dispatcher dispatcher, Microsoft.Extensions.Logging.ILogger<DeliveryProcessor> logger)
{
    /// <summary>Processes and disposes a delivery before returning, including on cancellation or failed settlement.</summary>
    /// <remarks>A failed settlement is never followed by another settlement attempt.</remarks>
    public async Task ProcessAsync(ConsumerDescriptor descriptor, IDelivery delivery, CancellationToken cancellationToken)
    {
        await using (delivery)
        {
            DispatchResult result;
            try
            {
                if (delivery.DecodeError is { } error) throw error;
                var context = new DispatchContext(descriptor, delivery.Payload.ToArray(), delivery.Headers);
                await dispatcher.DispatchAsync(context, cancellationToken);
                result = context.Result;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { return; }
            catch (Exception error) { result = DispatchResult.Release(error); }

            try
            {
                if (await delivery.SettleAsync(result, cancellationToken))
                    Microsoft.Extensions.Logging.LoggerExtensions.LogDebug(logger,
                        "InterBus: delivery for {Consumer} settled with {Outcome}", descriptor.ConsumerType, result.Kind);
                else
                    Microsoft.Extensions.Logging.LoggerExtensions.LogWarning(logger,
                        "InterBus: settlement rejected for {Consumer}", descriptor.ConsumerType);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
            catch (Exception error)
            {
                Microsoft.Extensions.Logging.LoggerExtensions.LogError(logger, error,
                    "InterBus: settlement failed for {Consumer}; allowing transport recovery", descriptor.ConsumerType);
            }
        }
    }
}
