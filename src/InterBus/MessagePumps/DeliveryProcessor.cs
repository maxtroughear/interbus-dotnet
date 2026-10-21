using Microsoft.Extensions.Logging;
using InterBus.Consuming;
using InterBus.Dispatching;

namespace InterBus.MessagePumps;

/// <summary>Runs the shared scoped dispatch pipeline and settles an owned delivery.</summary>
public sealed class DeliveryProcessor(Dispatcher dispatcher, ILogger<DeliveryProcessor> logger)
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
                if (delivery.DecodeError is { } error)
                    throw error;
                var context = new DispatchContext(descriptor, delivery.Payload.ToArray(), delivery.Headers);
                await dispatcher.DispatchAsync(context, cancellationToken);
                result = context.Result;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception error)
            {
                result = DispatchResult.Release(error);
            }

            try
            {
                if (await delivery.SettleAsync(result, cancellationToken))
                {
                    if (result.Kind == DispatchResultKind.Success)
                        logger.LogDebug("InterBus: delivery for {Consumer} acknowledged", descriptor.ConsumerType);
                    else
                        logger.LogWarning(result.Exception, "InterBus: delivery for {Consumer} settled with {Outcome}", descriptor.ConsumerType, result.Kind);
                }
                else
                    logger.LogWarning("InterBus: settlement rejected for {Consumer}", descriptor.ConsumerType);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
            }
            catch (Exception error)
            {
                logger.LogError(error, "InterBus: settlement failed for {Consumer}; allowing transport recovery", descriptor.ConsumerType);
            }
        }
    }
}
